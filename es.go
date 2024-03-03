package elasticsearch7

import (
	"context"
	"github.com/olivere/elastic/v7"
	"strconv"
)

var (
	esClient *elastic.Client
	ctx      = context.Background()
	esUrl    = "http://127.0.0.1:9200"
)

// 实例化es客户端
func EsInit() {
	var err error
	esClient, err = elastic.NewClient(elastic.SetURL(esUrl), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}
}

// 判断索引是否存在
func ExistIndex(indexName string) (bool, error) {
	exists, err := esClient.IndexExists(indexName).Do(ctx)
	return exists, err
}

// 创建索引
// indexName 索引名称,mapping 映射的结构体
func CreateIndex(indexName string, mapping string) (*elastic.IndicesCreateResult, error) {
	createIndex, err := esClient.CreateIndex(indexName).BodyString(mapping).Do(ctx)
	return createIndex, err
}

// 向索引写入单条数据
func AddDocToIndex(indexName string, id int, doc interface{}) error {
	_, err := esClient.Index().
		Index(indexName).
		Id(strconv.Itoa(id)).
		BodyJson(doc).
		Do(ctx)
	return err
}

// 根据文档id查询数据
func SearchDocByDocID(indexName string, id int) (*elastic.GetResult, error) {
	result, err := esClient.Get().
		Index(indexName).
		Id(strconv.Itoa(id)).
		Do(ctx)
	return result, err
}

// 精确查询,term是精确查询，字段类型keyword 不能是text
func TermQuery(indexName, field, value string, offset, limit int) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermQuery(field, value)
	result, err := esClient.Search().
		Index(indexName).
		Query(termQuery).
		From(offset).Size(limit).
		Pretty(true).
		Do(ctx)
	return result, err
}

// 通过文档ID更改信息
func UpdateByDocId(indexName string, id int, doc interface{}) error {
	_, err := esClient.Update().
		Index(indexName).
		Id(strconv.Itoa(id)).
		Doc(doc).
		Do(ctx)

	return err
}

// 词项多条件精确查询
func TermsQuery(indexName, field string, offset, limit int, values ...interface{}) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermsQuery(field, values...)
	result, err := esClient.Search().
		Index(indexName).
		Query(termQuery).
		From(offset).Size(limit).
		Pretty(true).
		Do(ctx)
	return result, err
}

// 词项的区间查询
func RangeQuery(indexName, field string, offset, limit int, gte, lte interface{}) (*elastic.SearchResult, error) {
	rangeQuery := elastic.NewRangeQuery(field).Gte(gte).Lte(lte)
	result, err := esClient.Search().
		Index(indexName).
		Query(rangeQuery).
		From(offset).Size(limit).
		Pretty(true).
		Do(ctx)
	return result, err
}

// 高亮搜索
func SearchWithHighlight(indexName, field, msg string, offset, limit int) (*elastic.SearchResult, error) {
	query := elastic.NewMatchQuery(field, msg)
	highlight := elastic.NewHighlight().Field("message")
	highlight.PreTags("<span color='red'>")
	highlight.PostTags("</span>")
	result, err := esClient.Search().
		Index(indexName).
		Query(query).
		Highlight(highlight).
		From(offset).Size(limit).
		Pretty(true).
		Do(ctx)
	return result, err
}
