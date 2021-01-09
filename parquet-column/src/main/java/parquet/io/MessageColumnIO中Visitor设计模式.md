* 在MessageColumnIO.getRecordReader(Filter filter)中使用了Visitor设计模式。
* 实现的需求，根据传入的filter类型，返回不同的Reader对象
* 代码实现

```java
class MessageColumnIO {
  Reader getRecordReader(Filter filter) {
    return filter.accept(new Visitor(){
      Reader visitor(FilterType1 filter) { return new Reader1(); }
      Reader visitor(FilterType2 filter) { return new Reader2(); }
    });
  }
}      

class Filter {
  Reader accept(Visitor visitor) { return visitor.visitor(this); } 
}
```