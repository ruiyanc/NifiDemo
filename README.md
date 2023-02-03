## 自定义Apache-Nifi组件

### 基础说明文档
1. 目录说明
    * `nifi-custom-nar`是打包目录
    * `nifi-custom-processors`是自定义处理器代码目录
    * 自定义的处理器需要在resources下的`org.apache.nifi.processor.Processor`新增一行Java文件名称才启用
2. 开发说明
    * 新增的Java文件需要继承AbstractProcessor类
    * init方法初始化参数，onScheduled周期执行的类，onTrigger承上启下执行的类
