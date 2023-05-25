# 版本更新内容
## 02
1. 知识库文件名配置化
2. 增加ip,domain,app打标签功能
3. 优化ETL逻辑：每个任务增加catch exception
## 02.rc
1. 修改知识库更新逻辑：每次更新删除旧的缓存
2. 修改common_app_label字段的补全逻辑：直接从app_id中解析app_name字段
3. 修复ioc检测的bug
## 06.rc1
1. 修改jackson为fastjson2