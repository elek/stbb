module github.com/elek/stbb

go 1.21

toolchain go1.23.1

require (
	github.com/alecthomas/kong v0.7.1
	github.com/dgraph-io/badger/v4 v4.2.0
	github.com/elek/storj-badger-storage v0.0.0-20240627194528-7392d8eb933f
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.4
	github.com/jackc/pgx/v5 v5.6.0
	github.com/klauspost/compress v1.17.7
	github.com/klauspost/cpuid/v2 v2.2.5
	github.com/minio/sha256-simd v1.0.0
	github.com/oschwald/maxminddb-golang v1.12.0
	github.com/outcaste-io/badger/v3 v3.2202.1-0.20220426173331-b25bc764af0d
	github.com/pkg/errors v0.9.1
	github.com/spacemonkeygo/monkit/v3 v3.0.23
	github.com/spf13/cobra v1.8.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.9.0
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	github.com/zeebo/blake3 v0.2.3
	github.com/zeebo/errs v1.4.0
	github.com/zeebo/errs/v2 v2.0.5
	github.com/zeebo/xxh3 v1.0.2
	go.uber.org/zap v1.27.0
	golang.org/x/benchmarks v0.0.0-20221122030604-0e4f958d02e0
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d
	golang.org/x/sys v0.26.0
	google.golang.org/protobuf v1.35.1
	gopkg.in/yaml.v3 v3.0.1
	lukechampine.com/blake3 v1.1.7
	storj.io/common v0.0.0-20241107064117-6b3c20bcf19b
	storj.io/drpc v0.0.35-0.20240709171858-0075ac871661
	storj.io/edge v1.75.1-0.20240328060906-2856366f2015
	storj.io/infectious v0.0.2
	storj.io/monkit-jaeger v0.0.0-20240221095020-52b0792fa6cd
	storj.io/storj v1.91.0-alpha.0.20240910152433-9e85d28d3efd
	storj.io/uplink v1.13.2-0.20240913115229-e8ce0e91b41f
)

require (
	cel.dev/expr v0.16.0 // indirect
	cloud.google.com/go v0.116.0 // indirect
	cloud.google.com/go/auth v0.9.8 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.4 // indirect
	cloud.google.com/go/bigquery v1.63.1 // indirect
	cloud.google.com/go/compute/metadata v0.5.2 // indirect
	cloud.google.com/go/iam v1.2.1 // indirect
	cloud.google.com/go/longrunning v0.6.1 // indirect
	cloud.google.com/go/monitoring v1.21.1 // indirect
	cloud.google.com/go/secretmanager v1.14.1 // indirect
	cloud.google.com/go/spanner v1.70.0 // indirect
	github.com/GoogleCloudPlatform/grpc-gcp-go/grpcgcp v1.5.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.24.1 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/apache/thrift v0.17.0 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/bmkessler/fastdiv v0.0.0-20190227075523-41d5178f2044 // indirect
	github.com/boombuler/barcode v1.0.1 // indirect
	github.com/calebcase/tmpfile v1.0.3 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudfoundry/gosigar v1.1.0 // indirect
	github.com/cncf/xds/go v0.0.0-20240822171458-6449f94b4d59 // indirect
	github.com/coreos/go-oidc/v3 v3.11.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/envoyproxy/go-control-plane v0.13.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/flynn/noise v1.0.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-jose/go-jose/v4 v4.0.2 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-oauth2/oauth2/v4 v4.4.2 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang/glog v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v23.5.26+incompatible // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/google/pprof v0.0.0-20230728192033-2ba5b33183c6 // indirect
	github.com/google/s2a-go v0.1.8 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
	github.com/googleapis/gax-go/v2 v2.13.0 // indirect
	github.com/googleapis/go-sql-spanner v1.7.4 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/gorilla/schema v1.2.0 // indirect
	github.com/gorilla/websocket v1.5.1 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgerrcode v0.0.0-20201024163028-a0d42d470451 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.14.1 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jtolds/monkit-hw/v2 v2.0.0-20191108235325-141a0da276b3 // indirect
	github.com/jtolds/tracetagger/v2 v2.0.0-rc5 // indirect
	github.com/jtolio/crawlspace v0.0.0-20231116162947-3ec5cc6b36c5 // indirect
	github.com/jtolio/crawlspace/tools v0.0.0-20231116162947-3ec5cc6b36c5 // indirect
	github.com/jtolio/mito v0.0.0-20230523171229-d78ef06bb77b // indirect
	github.com/jtolio/noiseconn v0.0.0-20230301220541-88105e6c8ac6 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/onsi/ginkgo/v2 v2.9.5 // indirect
	github.com/outcaste-io/ristretto v0.2.0 // indirect
	github.com/pelletier/go-toml/v2 v2.1.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/pquerna/otp v1.3.0 // indirect
	github.com/quic-go/qtls-go1-20 v0.4.1 // indirect
	github.com/quic-go/quic-go v0.40.1 // indirect
	github.com/redis/go-redis/v9 v9.5.1 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/segmentio/backo-go v0.0.0-20200129164019-23eae7c10bd3 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spacemonkeygo/spacelog v0.0.0-20180420211403-2296661a0572 // indirect
	github.com/spacemonkeygo/tlshowdy v0.0.0-20160207005338-8fa2cec1d7cd // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/spf13/viper v1.18.2 // indirect
	github.com/stripe/stripe-go/v75 v75.8.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/tidwall/gjson v1.17.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	github.com/zeebo/admission/v3 v3.0.3 // indirect
	github.com/zeebo/float16 v0.1.0 // indirect
	github.com/zeebo/goof v0.0.0-20230907150950-e9457bc94477 // indirect
	github.com/zeebo/incenc v0.0.0-20180505221441-0d92902eec54 // indirect
	github.com/zeebo/mwc v0.0.6 // indirect
	github.com/zeebo/structs v1.0.3-0.20230601144555-f2db46069602 // indirect
	github.com/zeebo/sudo v1.0.2 // indirect
	github.com/zyedidia/generic v1.2.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.29.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.54.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.54.0 // indirect
	go.opentelemetry.io/otel v1.29.0 // indirect
	go.opentelemetry.io/otel/metric v1.29.0 // indirect
	go.opentelemetry.io/otel/sdk v1.29.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.29.0 // indirect
	go.opentelemetry.io/otel/trace v1.29.0 // indirect
	go.uber.org/mock v0.3.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.28.0 // indirect
	golang.org/x/mod v0.20.0 // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/oauth2 v0.23.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/time v0.7.0 // indirect
	golang.org/x/tools v0.24.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/api v0.201.0 // indirect
	google.golang.org/genproto v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241007155032-5fefd90f89a9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/grpc v1.67.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/segmentio/analytics-go.v3 v3.1.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	storj.io/eventkit v0.0.0-20240415002644-1d9596fee086 // indirect
	storj.io/picobuf v0.0.3 // indirect
)

replace storj.io/storj => github.com/elek/storj v0.12.1-0.20241111103357-402a4c4ac315
