## 整体介绍
在安全领域无论是信息基础安全还是风控安全，事件模式捕捉属于很重要的部分。配置上支持模式表达和条件表达分离的策略，方便在线编排事件模式下发到flink集群。

* 场景

![image](https://github.com/huzhanchi/flink-cep/blob/main/images/intro.png)
* 改进

![image](https://github.com/huzhanchi/flink-cep/blob/main/images/enhance.png)
* 支持用正则表达式表达模式
```aidl
//a，b，c，d，e 五个事件有三个在五分钟内发生就匹配成功
[abcde]{3}within{300}
```
* 支持扁平化的groovy表达模式
```aidl
//事件a，b，c 三个事件在2分钟内发生1到2次则匹配成功
Flow.<JSONObject>begin("start").where("${a}").followedBy("next").where("${b}").or("${c}").times(1,2).within(120).getPattern()
```
* 支持自定义安全函数，用于条件表达式
```aidl
//定义a事件为 bash操作事件的ip为恶意ip 的操作事件则匹配成功，此处a事件是由条件表达式定义
a="security_ip(bash.ip) == 0"
Flow.<JSONObject>begin("start").where("${a}").getPattern()
```
## 组件介绍
### my-flink-cep
此部分基于apache flink cep做了扩展，主要扩展逻辑请关注com.my.security.shaded.flink.cep.operator.CepOperator，此项目中带有“扩展”即为增强功能

* 增加动态pattern刷新到cep算子
* 支持单算子下单事件流匹配多模式

### my-cep-common
此部分是配置相关实现
* 扁平groovy配置实现
* 正则到模式的转换实现
* 自定义安全表达式的实现

### my-cep-engine
最终提交任务的引擎部分，主要定义flink事件流的算子组装，从哪里来，如何动态加载模式，匹配的结果去向哪里。

## 使用介绍
共三个通用组件，可以根据具体安全场景需要使用。动态策略部分剥离到配置后台系统，对于一个新的安全场景需要做的是
* 注入动态下发逻辑
```java
PatternStream<Event> patternStream = CEP.injectPattern(new PatternInjectFunction() {
    @Override
    public void initialize() {
        //配置源初始化
    }

    @Override
    public Long getPeriod() {
        //返回轮询周期，单位S
        return 60L;
    }

    @Override
    public Pattern<Event, ?> inject() {
        //动态生成pattern
    }
})
```
* 定义安全领域函数
```java
public class SecurityIpFunction extends AbstractFunction {
    @Override
    public String getName() {
        return "security_ip";
    }
    //others...
}
```

* 编排事件模式表达
```aidl
a="security_ip(bash.ip) == 0"
[abcde]{3}within{300}
```
