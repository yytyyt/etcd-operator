package backup

import (
	"context"
	"flag"
	"fmt"
	clientv32 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/snapshot"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	etcdv1alpha1 "github.com/yytyyt/etcd-operator/api/v1alpha1"
	"github.com/yytyyt/etcd-operator/pkg/file"
	"go.etcd.io/etcd/clientv3"
	"os"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"time"
)

func logErr(log logr.Logger, err error, message string) error {
	log.Error(err, message)
	return fmt.Errorf("%s:%s", message, err)
}

func main() {
	var (
		backupTempDir      string
		etcdUrl            string
		dialTimeoutSeconds int64
		timeoutSeconds     int64
		backupUrl          string
	)
	flag.StringVar(&backupTempDir, "backup-tmp-dir", os.TempDir(), "The directory to temp place backup etcd cluster")
	flag.StringVar(&etcdUrl, "etcd-url", "", "URL for backup etcd")
	flag.StringVar(&backupUrl, "backup-url", "", "URL for backup etcd object storage")
	flag.Int64Var(&dialTimeoutSeconds, "dial-timeout-seconds", 5, "Timeout for dialing the Etcd")
	flag.Int64Var(&timeoutSeconds, "timeout-seconds", 60, "Timeout for backup the Etcd")
	flag.Parse()

	zapLogger := zap.NewRaw(zap.UseDevMode(true))
	ctrl.SetLogger(zapr.NewLogger(zapLogger))

	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer ctxCancel()

	log := ctrl.Log.WithName("backup").WithValues("etcd-url", etcdUrl)
	log.Info("Connecting to Etcd and getting snapshot data")

	// 定义一个本地的数据目录
	localPath := filepath.Join(backupTempDir, "snapshot.db")

	// 创建etcd snapshot manager
	etcdManager := snapshot.NewV3(zapLogger)

	// 数据保存到本地成功
	// 保存etcd snapshot 数据到licalpath
	err := etcdManager.Save(ctx, clientv32.Config(clientv3.Config{
		Endpoints:   []string{etcdUrl},
		DialTimeout: time.Duration(dialTimeoutSeconds) * time.Second,
	}), localPath)
	if err != nil {
		panic(logErr(log, err, "failed to get etcd snapshot data"))
	}

	// 根据storageType 来决定上传数据到什么地方去
	storageType, bucketName, objectName, err := file.ParseBackupUrl(backupUrl)
	if err != nil {
		panic(logErr(log, err, "failed to upload backup"))
	}

	switch storageType {
	case string(etcdv1alpha1.BackupStorageTypeS3):
		log.Info("Uploading snapshot")
		size, err := handleS3(ctx, bucketName, objectName, localPath)
		if err != nil {
			panic(logErr(log, err, "failed to upload backup"))
		}
		log.WithValues("upload-size", size).Info("Backup complete")
	case string(etcdv1alpha1.BackupStorageTypeOSS):
		// TODO
	default:
		panic(logErr(log, fmt.Errorf("storage type error"), fmt.Sprintf("unknown storage type:%v", storageType)))
	}

}

func handleS3(ctx context.Context, bucketName, objectName, localPath string) (int64, error) {
	// 接下来就上传
	// TODO 根据传递进来的参数判断初始化s3还是oss
	endpoint := os.Getenv("ENDPOINT")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")

	/*endpoint := "play.min.io"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"*/
	s3Uploader := file.NewS3Uploader(endpoint, accessKeyID, secretAccessKey)

	return s3Uploader.Upload(ctx, bucketName, objectName, localPath)

}
