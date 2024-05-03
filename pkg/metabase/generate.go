package metabase

type Generate struct {
	Connection string `arg:""`
}

func (t *Generate) Run() error {
	//segments := make([]metabase.RawSegment, 1000)
	//for i := range segments {
	//	obj := metabasetest.RandObjectStream()
	//	segments[i] = metabasetest.DefaultRawSegment(obj, metabase.SegmentPosition{})
	//}
	//ctx := context.TODO()
	//adapter, err := metabase.NewSpannerAdapter(ctx, metabase.SpannerConfig{
	//	Database: t.Connection,
	//})
	//
	//aliasCahce := metabase.NewNodeAliasCache(adapter)
	//err = adapter.TestingBatchInsertSegments(ctx, aliasCahce, segments)
	//return err
	return nil
}
