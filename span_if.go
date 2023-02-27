package common

//span interface define, no need to import jaeger
type Span interface {
	SetTag(k string, v interface{}) Span
	LogKV(k interface{}, v interface{})
	Finish()
	PbContext() interface{}
	ChildSpan(opName string) Span
}
