# PageType

* DATA_PAGE(0),
* INDEX_PAGE(1),
* DICTIONARY_PAGE(2),
* DATA_PAGE_V2(3);

PageHeader数据结构
```java
  public PageType type; // required
  public int uncompressed_page_size; // required
  public int compressed_page_size; // required
  public int crc; // optional
  public DataPageHeader data_page_header; // optional
  public IndexPageHeader index_page_header; // optional
  public DictionaryPageHeader dictionary_page_header; // optional
  public DataPageHeaderV2 data_page_header_v2; // optional
```