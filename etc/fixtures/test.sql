GRANT RELOAD, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "sphinx"@"%";
FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS `test` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`country` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`region` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `country_id` int(10) unsigned NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `country_id` (`country_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`town` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `region_id` int(10) unsigned NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `region_id` (`region_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`language` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`catalogue` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `parent_id` int(10) unsigned DEFAULT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `parent_id` (`parent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`vacancy` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `town_id` int(10) unsigned NOT NULL,
  `profession` varchar(255) NOT NULL,
  `description` mediumtext NOT NULL,
  `payment_from` int(10) unsigned NOT NULL DEFAULT '0',
  `payment_to` int(10) unsigned NOT NULL DEFAULT '0',
  `latitude` float(9,6) NOT NULL,
  `longitude` float(9,6) NOT NULL,
  `is_archive` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `town_id` (`town_id`),
  KEY `is_archive` (is_archive)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`vacancy_language` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `vacancy_id` int(10) unsigned NOT NULL,
  `language_id` int(10) unsigned NOT NULL,
  `language_level` tinyint(3) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `vacancy_language` (`vacancy_id`,`language_id`),
  KEY `language_id` (`language_id`,`language_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE `test`.`vacancy_catalogue` (
  `vacancy_id` int(10) unsigned NOT NULL,
  `catalogue_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`vacancy_id`, `catalogue_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO `test`.`country` (`id`, `name`) VALUES (1, "Россия");

INSERT INTO `test`.`region` (`id`, `country_id`, `name`) VALUES
(1, 1, "Московская область"),
(2, 1, "Ленинградская область")
;

INSERT INTO `test`.`town` (`id`, `region_id`, `name`) VALUES
(1, 1, "Москва"),
(2, 2, "Санкт-Петербург")
;

INSERT INTO `test`.`language` (`id`, `name`) VALUES
(1, "Английский язык"),
(2, "Немецкий язык")
;

INSERT INTO `test`.`catalogue` (`id`, `parent_id`, `name`) VALUES
(1, null, "IT, Интернет, связь, телеком"),
(2, 1, "Разработка, программирование"),
(3, 1, "Интернет, создание и поддержка сайтов"),
(4, 1, "Администрирование баз данных"),
(5, null, "Продажи"),
(6, 5, "Товары народного потребления"),
(7, 5, "Продукты питания"),
(8, 5, "Оптовая торговля"),
(9, 5, "Автомобили, запчасти")
;

INSERT INTO `test`.`vacancy` (`id`, `town_id`, `profession`, `description`, `payment_from`, `payment_to`, `latitude`, `longitude`, `is_archive`, `created_at`, `updated_at`) VALUES
(1, 1, "Программист PHP", "Писать сайты на PHP, bitrix и wordpress", 100000, 150000, 55.751702, 37.629322, 0, "2019-02-26 16:00:00", "2019-02-26 16:30:00"),
(2, 1, "Программист Python", "Писать AI и ML на python 2.7", 150000, 200000, 55.731651, 37.599807, 0, "2019-02-26 18:10:00", "2019-02-26 19:45:00"),
(3, 1, "Программист Golang", "Писать микросервисы на Golang, знать docker и kubernetes", 180000, 220000, 55.614536, 37.682542, 0, "2019-02-26 18:00:00", "2019-02-26 18:15:00"),
(4, 1, "Программист Javascript", "Писать сайты, AI и ML, микросервисы на java script, знать css, уметь настраивать webpack", 50000, 300000, 55.831190, 37.627487, 0, "2019-02-26 19:20:00", "2019-02-26 19:50:00"),
(5, 1, "Программист Perl", "Писать сайты и микросервисы на perl, знать golang", 100000, 150000, 55.747669, 37.474542, 1, "2019-02-26 19:30:00", "2019-02-26 21:50:00"),
(6, 1, "Администратор MSSQL", "Знание устройства MS SQL, теории баз данных и языка T-SQL", 80000, 130000, 55.753330, 37.419820, 0, "2019-02-26 19:20:00", "2019-02-26 19:50:00"),
(7, 1, "DevOps / DBA PostgreSQL", "Поддержка и развитие CI/CD сервисов. Опыт работы и администрирования DB (MSSQL/ Postgres)", 100000, 140000, 55.673580, 37.677907, 0, "2019-02-26 17:35:00", "2019-02-26 19:10:00"),
(8, 2, "Продавец мороженого", "Продажа мороженого в фирменных киосках, консультация покупателей", 50000, 55000, 59.936383, 30.302240, 1, "2019-02-26 10:25:00", "2019-02-26 16:50:00"),
(9, 2, "Продавец сладостей", "Продажа кондитерских изделий, конфет, выпечки", 55000, 60000, 59.945892, 30.336953, 0, "2019-02-26 14:30:00", "2019-02-26 19:05:00"),
(10, 2, "Продавец торгового зала", "Производить выкладку товаров в торговом зале; контролировать сроки годности товаров", 60000, 70000, 59.945445, 30.371044, 0, "2019-02-26 11:55:00", "2019-02-26 18:35:00"),
(11, 2, "Продавец-кассир", "Обслуживание покупателей на кассе (кассир) и в торговом зале (продавец)", 50000, 60000, 59.974134, 30.237041, 0, "2019-02-26 17:05:00", "2019-02-26 19:40:00"),
(12, 2, "Менеджер по продажам", "Развитие и ведение клиентской базы, проведение переговоров с потенциальными и имеющимися клиентами", 60000, 90000, 59.948534, 30.317236, 0, "2019-02-26 09:15:00", "2019-02-26 13:40:00"),
(13, 2, "Менеджер оптовых продаж", "Прямые оптовые продажи в категории, развитие оптовой клиентской базы", 70000, 120000, 59.929450, 30.287413, 1, "2019-02-26 18:10:00", "2019-02-26 21:30:00"),
(14, 2, "Менеджер по оптовым продажам", "Продажи, привлечение оптовых покупателей в городах миллионниках", 75000, 125000, 59.979272, 30.270038, 0, "2019-02-26 08:20:00", "2019-02-26 16:45:00")
;

INSERT INTO `test`.`vacancy_language` (`id`, `vacancy_id`, `language_id`, `language_level`) VALUES
(1, 1, 1, 2),
(2, 2, 1, 2),
(3, 3, 1, 2),
(4, 4, 1, 2),
(5, 6, 1, 2),
(6, 7, 1, 2),
(7, 7, 2, 2),
(8, 13, 1, 1),
(9, 14, 1, 1),
(10, 14, 2, 1)
;

INSERT INTO `test`.`vacancy_catalogue` (`vacancy_id`, `catalogue_id`) VALUES
(1, 2), (1, 3),
(2, 2),
(3, 2), (3, 3),
(4, 2), (4, 3),
(5, 2), (5, 3),
(6, 4),
(7, 3), (7, 4),
(8, 6), (8, 7),
(9, 6), (9, 7),
(10, 6), (10, 7),
(11, 6), (11, 7),
(12, 8), (12, 9),
(13, 8), (13, 9),
(14, 8)
;
