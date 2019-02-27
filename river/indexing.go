package river

import (
	"bytes"
	"context"
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"text/template"

	set "github.com/deckarep/golang-set"
	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	"github.com/superjobru/go-mysql-sphinx/util"
	"gopkg.in/birkirb/loggers.v1"
)

const indexerConfigTemplate = `
indexer {
{{- with .Indexer}}
	{{- if .MemLimit}}
	mem_limit = {{.MemLimit}}
	{{- end}}
	{{- if ne "" .MaxIops}}
	max_iops = {{.MaxIops}}
	{{- end}}
	{{- if ne "" .MaxIosize}}
	max_iosize = {{.MaxIosize}}
	{{- end}}
	{{- if ne "" .MaxXmlpipe2Field}}
	max_xmlpipe2_field = {{.MaxXmlpipe2Field}}
	{{- end}}
	{{- if ne "" .WriteBuffer}}
	write_buffer = {{.WriteBuffer}}
	{{- end}}
	{{- if ne "" .MaxFileFieldBuffer}}
	max_file_field_buffer = {{.MaxFileFieldBuffer}}
	{{- end}}
	{{- if ne "" .OnFileFieldError}}
	on_file_field_error = {{.OnFileFieldError}}
	{{- end}}
	{{- if ne "" .LemmatizerCache}}
	lemmatizer_cache = {{.LemmatizerCache}}
	{{- end}}
{{- end}}
}

source {{.Name}}
{
	type = mysql
{{- with .Mysql}}
	sql_host = {{.Host}}
	sql_port = {{.Port}}
	sql_user = {{.User}}
	sql_pass = {{.Pass}}

	sql_query_pre = SET NAMES {{.Charset}}
{{- end}}
{{- with .Indexer}}
	{{- if gt .GroupConcatMaxLen 0}}
	sql_query_pre = SET group_concat_max_len = {{.GroupConcatMaxLen}}
	{{- end}}
{{- end}}

	sql_db =
	sql_query = {{.Query}}
{{range .Fields}}
	{{fieldspec .}}
{{- end}}
}

index {{.Name}}
{
	type = plain
{{- with .Indexer.Dictionaries}}
	{{- if ne "" .Exceptions}}
	exceptions = {{tmpname .Exceptions}}
	{{- end}}
	{{- if ne "" .Stopwords}}
	stopwords = {{tmpname .Stopwords}}
	{{- end}}
	{{- if ne "" .Wordforms}}
	wordforms = {{tmpname .Wordforms}}
	{{- end}}
{{- end}}
	{{.Indexer.Tokenizer}}
	path = {{.Name}}.new
	source = {{.Name}}
}
`

type indexGroupBuild struct {
	id           string
	indexes      []indexBuild
	uploader     IndexUploaderConfig
	logger       loggers.Advanced
	rebuildState *RebuildStartState
}

type indexBuild struct {
	id          string
	index       string
	chunks      []indexChunkBuild
	parts       uint16
	storagePath string
}

type indexChunkBuild struct {
	config     []byte
	files      []string
	plainIndex string
	rtIndex    string
	buildID    string
}

type indexChunkBuildResult struct {
	dir string
	err error
}

type indexUploadArgs struct {
	executable string
	args       []string
}

// UploaderArgSubst substitutions allowed in arguments for uploader
type UploaderArgSubst struct {
	BuildID string
	DataDir string
	Index   string
	Host    string
}

type RebuildStartState struct {
	gtid mysql.GTIDSet
	pos  mysql.Position
}

func (s RebuildStartState) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	return s.gtid, nil
}

func (s RebuildStartState) GetMasterPos() (mysql.Position, error) {
	return s.pos, nil
}

func NewRebuildStartState(c *canal.Canal) (*RebuildStartState, error) {
	gtid, err := c.GetMasterGTIDSet()
	if err != nil {
		return nil, err
	}

	pos, err := c.GetMasterPos()
	if err != nil {
		return nil, err
	}

	return &RebuildStartState{gtid, pos}, err
}

var errIndexingInProgress = errors.New("index is already being built")
var errDataSourceNotFound = errors.New("data source for the requested index not found")
var errMissingIDGenerator = errors.New("id generator is nil")

func indexFieldSpec(field IndexConfigField) string {
	switch field.Type {
	case AttrMulti64:
		return fmt.Sprintf("sql_attr_multi = bigint %s from field", field.Name)
	case AttrMulti:
		return fmt.Sprintf("sql_attr_multi = uint %s from field", field.Name)
	case DocID, TextField:
		return fmt.Sprintf("# sql_%s = %s", field.Type, field.Name)
	default:
		return fmt.Sprintf("sql_%s = %s", field.Type, field.Name)
	}
}

func newIndexBuild(c *Config, index string, idGen func() uuid.UUID) (*indexBuild, error) {
	dataSource, exists := c.DataSource[index]
	if !exists {
		return nil, errors.Trace(errDataSourceNotFound)
	}
	funcs := map[string]interface{}{
		"tmpname":   func(args ...string) string { return "" },
		"fieldspec": indexFieldSpec,
	}
	tpl, err := template.New(index).Funcs(funcs).Parse(indexerConfigTemplate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m, err := newIndexerMysqlSettings(c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := buildIndexerSettings(*m, index, *dataSource)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if idGen == nil {
		return nil, errors.Trace(errMissingIDGenerator)
	}
	buildID := idGen().String()
	chunks := make([]indexChunkBuild, dataSource.Parts)
	for i, d := range data {
		funcs := map[string]interface{}{
			"tmpname": func(f string) string {
				return util.DictionaryTmpFilename(d.Name, f)
			},
		}
		var buf bytes.Buffer
		if err := tpl.Funcs(funcs).Execute(&buf, d); err != nil {
			return nil, errors.Trace(err)
		}
		dicts := dataSource.Indexer.Dictionaries
		chunks[i] = indexChunkBuild{
			buildID: buildID,
			config:  buf.Bytes(),
			files: []string{
				os.ExpandEnv(dicts.Exceptions),
				os.ExpandEnv(dicts.Stopwords),
				os.ExpandEnv(dicts.Wordforms),
			},
			plainIndex: d.Name,
			rtIndex:    d.RTName,
		}
	}
	return &indexBuild{
		id:          buildID,
		index:       index,
		chunks:      chunks,
		parts:       dataSource.Parts,
		storagePath: dataSource.StoragePath,
	}, nil
}

func newIndexGroupBuild(
	c *Config,
	logger loggers.Contextual,
	indexes []string,
	idGen func() uuid.UUID,
	rebuildState *RebuildStartState,
) (*indexGroupBuild, error) {
	if indexes == nil {
		indexes = []string{}
		for index := range c.DataSource {
			indexes = append(indexes, index)
		}
	}
	if idGen == nil {
		return nil, errors.Trace(errMissingIDGenerator)
	}
	buildID := idGen()
	childIDGen := func() uuid.UUID { return buildID }
	builds := make([]indexBuild, len(indexes))
	for i, index := range indexes {
		build, err := newIndexBuild(c, index, childIDGen)
		if err != nil {
			return nil, errors.Trace(err)
		}
		builds[i] = *build
	}
	build := &indexGroupBuild{
		id:           buildID.String(),
		indexes:      builds,
		uploader:     c.IndexUploader,
		rebuildState: rebuildState,
	}
	build.logger = logger.WithFields(
		"buildId", build.id,
		"indexSet", build.indexSet(),
	)
	return build, nil
}

func (build indexGroupBuild) indexSet() set.Set {
	indexes := set.NewSet()
	for _, b := range build.indexes {
		indexes.Add(b.index)
	}
	return indexes
}

func (build indexGroupBuild) indexSlice() []string {
	indexes := []string{}
	for _, b := range build.indexes {
		indexes = append(indexes, b.index)
	}
	return indexes
}

func buildIndex(ctx context.Context, logger loggers.Advanced, buildDir string, build indexBuild) ([]string, error) {
	var i int
	var err error
	in := make([]chan indexChunkBuild, len(build.chunks))
	out := make([]chan indexChunkBuildResult, len(build.chunks))
	doBuild := func(i int) {
		childCtx, cancel := context.WithCancel(ctx)
		defer close(out[i])
		defer cancel()
		for chunk := range in[i] {
			dir, err := buildIndexChunk(childCtx, logger, chunk, buildDir)
			out[i] <- indexChunkBuildResult{dir: dir, err: err}
		}
	}
	for i, chunk := range build.chunks {
		in[i] = make(chan indexChunkBuild)
		out[i] = make(chan indexChunkBuildResult)
		go doBuild(i)
		in[i] <- chunk
		close(in[i])
	}
	result := make([]indexChunkBuildResult, len(build.chunks))
	for i = range build.chunks {
		result[i] = <-out[i]
	}
	for _, res := range result {
		if res.err != nil {
			return nil, errors.Trace(res.err)
		}
	}
	err = os.MkdirAll(build.storagePath, 0755)
	if err != nil {
		return nil, errors.Trace(err)
	}
	indexFiles := make([]string, 0)
	for _, res := range result {
		files, err := filepath.Glob(filepath.Join(res.dir, "*"))
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, tmpFile := range files {
			storedFile := filepath.Join(build.storagePath, filepath.Base(tmpFile))
			err := os.Rename(tmpFile, storedFile)
			if err != nil {
				return nil, errors.Trace(err)
			}
			indexFiles = append(indexFiles, storedFile)
		}
	}
	return indexFiles, nil
}

func buildIndexChunk(ctx context.Context, logger loggers.Advanced, chunk indexChunkBuild, buildDir string) (string, error) {
	dir := filepath.Join(buildDir, chunk.rtIndex)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return "", errors.Annotatef(err, "could not create temporary directory for indexing")
	}
	return dir, buildIndexChunkWithDir(ctx, logger, chunk, dir)
}

func buildIndexChunkWithDir(ctx context.Context, logger loggers.Advanced, chunk indexChunkBuild, dir string) error {
	err := writeIndexBuildIDFile(dir, chunk)
	if err != nil {
		return errors.Trace(err)
	}
	configFile := fmt.Sprintf("%s.conf", chunk.plainIndex)
	err = writeIndexerConfigFile(dir, configFile, chunk.config)
	if err != nil {
		return errors.Trace(err)
	}
	for _, file := range chunk.files {
		newfile := util.DictionaryTmpFilename(chunk.plainIndex, file)
		err = util.Copy(file, filepath.Join(dir, newfile))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return runIndexer(ctx, logger, dir, configFile, chunk)
}

func writeIndexerConfigFile(dir, file string, config []byte) error {
	configFile := filepath.Join(dir, file)
	if err := ioutil.WriteFile(configFile, config, 0644); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func writeIndexBuildIDFile(dir string, chunk indexChunkBuild) error {
	fileName := filepath.Join(dir, fmt.Sprintf("%s.build-id", chunk.plainIndex))
	if err := ioutil.WriteFile(fileName, []byte(chunk.buildID+"\n"), 0644); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func runIndexer(ctx context.Context, logger loggers.Advanced, dir, configFile string, chunk indexChunkBuild) error {
	indexer, err := exec.LookPath("indexer")
	if err != nil {
		return errors.Annotatef(err, "indexer executable is not found in PATH")
	}
	stdoutFile := filepath.Join(dir, fmt.Sprintf("%s.stdout.log", chunk.plainIndex))
	stderrFile := filepath.Join(dir, fmt.Sprintf("%s.stderr.log", chunk.plainIndex))
	stdoutBuf := bytes.NewBuffer([]byte{})
	stderrBuf := bytes.NewBuffer([]byte{})
	stdout, err := os.OpenFile(stdoutFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Trace(err)
	}
	defer stdout.Close()
	stderr, err := os.OpenFile(stderrFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Trace(err)
	}
	defer stderr.Close()
	cmd := exec.Cmd{
		Path:   indexer,
		Args:   []string{indexer, "--all", "--config", configFile},
		Dir:    dir,
		Stdout: io.MultiWriter(stdout, stdoutBuf),
		Stderr: io.MultiWriter(stderr, stderrBuf),
	}
	err = cmd.Start()
	if err != nil {
		return errors.Annotate(err, "could not start indexer")
	}
	logger.Infof("[pid %d] %s", cmd.Process.Pid, strings.Join(cmd.Args, " "))
	err = waitForCommand(ctx, cmd, nil)
	if errors.Cause(err) != context.Canceled {
		err = errors.Annotatef(
			err,
			"indexer failed\n=== stdout ===\n%s\n\n=== stderr ===\n%s",
			stdoutBuf.String(),
			stderrBuf.String(),
		)
	}
	return err
}

func uploadIndexFiles(ctx context.Context, args indexUploadArgs, fileList []string, logger loggers.Advanced) error {
	command, err := exec.LookPath(args.executable)
	if err != nil {
		return errors.Annotatef(err, "uploader executable %s is not found in PATH", args.executable)
	}
	in := strings.NewReader(strings.Join(fileList, "\n"))
	out := new(bytes.Buffer)
	cmdArgs := []string{command}
	cmdArgs = append(cmdArgs, args.args...)
	cmd := exec.Cmd{
		Path:   command,
		Args:   cmdArgs,
		Stdin:  in,
		Stdout: out,
		Stderr: out,
	}
	err = cmd.Start()
	if err != nil {
		return errors.Annotate(err, "could not start uploader")
	}
	logger.Infof("[pid %d] %s ( uploading files: %s )", cmd.Process.Pid, strings.Join(cmd.Args, " "), strings.Join(fileList, " "))
	return errors.Trace(waitForCommand(ctx, cmd, out))
}

func expandUploaderArg(tpl TomlGoTemplate, values UploaderArgSubst) (string, error) {
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, values); err != nil {
		return "", errors.Trace(err)
	}
	return os.ExpandEnv(buf.String()), nil
}

func rebuildIndexGroup(ctx context.Context, r *River, build indexGroupBuild) error {
	indexSet := build.indexSet()
	if r.rebuildInProgress.Cardinality() > 0 {
		return errors.Annotatef(errIndexingInProgress, "in progress: %s", r.rebuildInProgress)
	}
	indexSet.Each(func(index interface{}) bool {
		r.rebuildInProgress.Add(index)
		return false
	})
	defer indexSet.Each(func(index interface{}) bool {
		r.rebuildInProgress.Remove(index)
		return false
	})
	err := r.enableBuildMode()
	if err != nil {
		return errors.Annotate(err, "pausing canal updates failed")
	}
	err = buildAndUploadIndexGroup(ctx, r.c, build)
	if err != nil {
		r.disableBuildMode()
		return errors.Trace(err)
	}
	r.stopSyncRoutine()
	err = r.sphinxService.ReloadRtIndex(build)
	if err != nil {
		return errors.Trace(err)
	}

	if build.rebuildState != nil {
		if err = r.master.resetToCurrent(build.rebuildState); err != nil {
			build.logger.Errorf("failed to reset GTID after successful rebuild: %s", errors.ErrorStack(err))
		} else {
			r.l.Infof("reset GTID after successful rebuild to: %s", build.rebuildState.gtid)
		}
	}

	r.startSyncRoutine()

	return nil
}

func buildAndUploadIndexGroup(ctx context.Context, config *Config, build indexGroupBuild) (err error) {
	files := []string{}
	buildDir := filepath.Join(config.DataDir, fmt.Sprintf("build-%s", build.id))
	err = os.MkdirAll(buildDir, 0755)
	if err != nil {
		return errors.Annotatef(err, "could not create build directory '%s'", buildDir)
	}
	for _, singleBuild := range build.indexes {
		singleIndexFiles, err := buildIndex(ctx, build.logger, buildDir, singleBuild)
		if err != nil {
			return errors.Annotatef(err, "error building index %s", singleBuild.index)
		}
		files = append(files, singleIndexFiles...)
	}
	os.RemoveAll(buildDir)
	hosts, err := makeHostListForUploading(config.SphAddr, build.uploader.HostMap)
	if err != nil {
		return errors.Annotatef(err, "error making list of hosts for uploading index files")
	}
	err = uploadIndexGroup(ctx, build, config.DataDir, hosts, files)
	if err != nil {
		return errors.Annotatef(err, "error uploading index files")
	}
	return
}

func makeHostListForUploading(hosts []string, hostMap map[string]string) ([]string, error) {
	result := make([]string, len(hosts))
	for i, addr := range hosts {
		host, _, err := splitAddr(addr, 0)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if mapping, exists := hostMap[host]; exists {
			result[i] = mapping
		} else {
			result[i] = host
		}
	}
	return result, nil
}

func uploadIndexGroup(ctx context.Context, build indexGroupBuild, dataDir string, hosts []string, fileList []string) (err error) {
	in := make([]chan indexUploadArgs, len(hosts))
	out := make([]chan error, len(hosts))
	doUpload := func(i int) {
		childCtx, cancel := context.WithCancel(ctx)
		defer close(out[i])
		defer cancel()
		for args := range in[i] {
			out[i] <- uploadIndexFiles(childCtx, args, fileList, build.logger)
		}
	}
	argsList := make([]indexUploadArgs, len(hosts))
	for i, host := range hosts {
		subst := UploaderArgSubst{
			DataDir: dataDir,
			Host:    host,
		}
		arguments := make([]string, len(build.uploader.Arguments))
		for i, arg := range build.uploader.Arguments {
			arguments[i], err = expandUploaderArg(arg, subst)
			if err != nil {
				return errors.Trace(err)
			}
		}
		argsList[i] = indexUploadArgs{
			executable: build.uploader.Executable,
			args:       arguments,
		}
	}
	for i, args := range argsList {
		in[i] = make(chan indexUploadArgs)
		out[i] = make(chan error)
		go doUpload(i)
		in[i] <- args
		close(in[i])
	}
	result := make([]error, len(argsList))
	for i := range out {
		result[i] = <-out[i]
	}
	for i, err := range result {
		if err != nil {
			return errors.Annotatef(err, "could not upload index files to %s", hosts[i])
		}
	}
	return nil
}

func waitForCommand(ctx context.Context, cmd exec.Cmd, out *bytes.Buffer) error {
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case err := <-done:
		if err != nil && out != nil {
			return errors.Annotatef(err, "command failed; output:\n%s\n", out.String())
		}
		return err
	case <-ctx.Done():
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
		}
		return errors.Trace(ctx.Err())
	}
}
