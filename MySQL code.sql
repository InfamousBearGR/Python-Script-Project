DROP DATABASE IF EXISTS python_data;
CREATE DATABASE python_data;
USE python_data;

CREATE TABLE nsatae_gr( 
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Nights spent at tourist accommodation establishments of Greece

CREATE TABLE nsbnratae_gr(
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Nights spent by non-residents at tourist accommodation establishments of Greece

CREATE TABLE aatae_gr(
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Arrivals at tourist accommodation establishments of Greece

CREATE TABLE aonratae_gr(
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Arrivals of non-residents at tourist accommodation establishments of Greece



CREATE TABLE nsatae_es( 
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Nights spent at tourist accommodation establishments of Spain

CREATE TABLE nsbnratae_es(
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Nights spent by non-residents at tourist accommodation establishments of Spain

CREATE TABLE aatae_es(
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Arrivals at tourist accommodation establishments of Spain

CREATE TABLE aonratae_es(
date DATE, 
number INT(10),
PRIMARY KEY(date)
); -- table for Arrivals of non-residents at tourist accommodation establishments of Spain
