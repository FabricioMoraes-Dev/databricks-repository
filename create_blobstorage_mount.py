# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('SCOPE_NAME')

# COMMAND ----------

dbutils.fs.mount(source = "wasbs://CONTAINER_NAME@STORAGE_ACCOUNT_NAME.blob.core.windows.net",
                 mount_point = "/mnt/MOUNT_NAME",
                 extra_configs = {'fs.azure.account.key.STORAGE_ACCOUNT_NAME.blob.core.windows.net':dbutils.secrets.get('SCOPE_NAME','SECRET_NAME')})

# COMMAND ----------

dbutils.fs.ls("/mnt/MOUNT_NAME")

# COMMAND ----------

dbutils.fs.unmount("/mnt/MOUNT_NAME")
