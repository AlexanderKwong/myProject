/**
 * Created by Kwong on 2017/11/10.
 */
package base.deal;
/**
 * 思考1
 * 对于 同一个集合：
 * 遍历一遍执行，对每个元素执行100行执行，和 遍历10遍，每次执行 10行指令的区别是什么
 * 没有想明白就不要往下做了。
 * 1、测试证明性能有差异
 */
/**
 * 思考2
 * 对于 不知道什么时候被触发的动作，如join , out 能否用回调实现
 */


/*
 * 实现数据的流处理
 */

//1条数据 -> 预处理 -> 关联 -> 统计1 -> 统计2 -> 统计3 -> ... （吐出）

/****************好处****************/
//      1、封装不同的统计逻辑
//      2、builder容易增加/删除/修改统计环节
//      3、对于spark调用，可去掉预处理和关联，更好的利用算子进行关联
/****************关键点**************/
//      1、入参必须是final的，且是不可变类（一旦创建无法变更其成员，成员也是递归如此）,
// 但是dealSample的过程中就是不断地给sample回填啊, 如果是不可变类的话岂不是要不断创建对象？！
//      2
//      3
/****************弊端****************/
//      1、假如有一种场景是多条线的，例如：
//        / -> 预处理A -> 关联A -> ..
// 1条数据 --> 预处理B -> ..B -> ..
//        \ -> 预处理C -> ..C -> ..
//   解决办法就是 搞两个 builder， 对于自关联 可采用 Tuple2<>处理

// 但是，当遇到一线分多线 或 多线合一线的情况呢？例如：
//                  / -> 关联A -> ..
// 1条数据 -> 预处理 --> 关联B -> ..
//                  \ -> 关联C -> ..
//
//                  / -> 关联A -> 处理n -\
// 1条数据 -> 预处理                      -> 关联C
//                 \ -> 关联B -> 处理m -/
// 上面的方式做关联，关联则不传递非自己该处理的入参类型的对象

// 另外，如果我想根据开关 来进行串联呢


/*************************  考虑注解 加 依赖注入来减少 mapper/reducer/main的编码量  ****************************/
 /************学习spring 的依赖注入模块****************/


 /**********************************/
 // 模块化编程——分package按模块分而不按 步骤分，类型内可按步骤分
