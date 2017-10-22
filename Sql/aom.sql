/*
Navicat PGSQL Data Transfer

Source Server         : localhost
Source Server Version : 90509
Source Host           : localhost:5432
Source Database       : aom
Source Schema         : public

Target Server Type    : PGSQL
Target Server Version : 90509
File Encoding         : 65001

Date: 2017-10-21 18:26:41
*/


-- ----------------------------
-- Table structure for aom_task_hosts_static
-- ----------------------------
DROP TABLE IF EXISTS "public"."aom_task_hosts_static";
CREATE TABLE "public"."aom_task_hosts_static" (
"id" varchar(32) COLLATE "default" NOT NULL,
"host" varchar(20) COLLATE "default",
"port" varchar(5) COLLATE "default",
"username" varchar(30) COLLATE "default",
"password" varchar(255) COLLATE "default"
)
WITH (OIDS=FALSE)

;

-- ----------------------------
-- Records of aom_task_hosts_static
-- ----------------------------

-- ----------------------------
-- Table structure for aom_task_schedule
-- ----------------------------
DROP TABLE IF EXISTS "public"."aom_task_schedule";
CREATE TABLE "public"."aom_task_schedule" (
"id" varchar(32) COLLATE "default" NOT NULL,
"parentid" varchar(32) COLLATE "default",
"name" varchar(64) COLLATE "default",
"module_name" varchar(32) COLLATE "default",
"module_oper" varchar(255) COLLATE "default",
"module_cron" varchar(255) COLLATE "default",
"module_order" int2
)
WITH (OIDS=FALSE)

;

-- ----------------------------
-- Records of aom_task_schedule
-- ----------------------------
INSERT INTO "public"."aom_task_schedule" VALUES ('1', '1', '127.0.0.1', '1001', '{"moduleid":"1001","host":"127.0.0.1","param":"{\"0\":\"nothing\"}","platform":"local"}', '{"type":"interval", "value":"20"}', '0');

-- ----------------------------
-- Alter Sequences Owned By 
-- ----------------------------

-- ----------------------------
-- Primary Key structure for table aom_task_hosts_static
-- ----------------------------
ALTER TABLE "public"."aom_task_hosts_static" ADD PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table aom_task_schedule
-- ----------------------------
ALTER TABLE "public"."aom_task_schedule" ADD PRIMARY KEY ("id");
