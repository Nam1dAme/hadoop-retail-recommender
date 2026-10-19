# 数据库迁移目录

W02 从 001 开始实现，规范见 [数据库设计](../docs/06-database.md)。当前没有已应用迁移，也没有创建数据库或角色。迁移前备份；已应用文件不可修改，schema_migration 保存校验和。reader、writer、migration 凭据按角色注入，不写入配置和 Git。
