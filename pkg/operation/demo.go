package operation

import (
	"context"
	"fmt"
	"git.qihoo.cloud/q8s/operator-test-etcd/pkg/xray"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"time"
)

var path = "/test"

//var host = flag.String("host", "10.217.62.28:32181", "The ip:port of zookeeper cluster.")

//var (
//	config clientv3.Config
//	client *clientv3.Client
//   err error
//	kv clientv3.KV
//)

func Run() {
	tlsInfo := transport.TLSInfo{
		CertFile: "/etc/etcdtls/operator/etcd-tls/etcd-client.crt",
		KeyFile:  "/etc/etcdtls/operator/etcd-tls/etcd-client.key",
		CAFile:   "/etc/etcdtls/operator/etcd-tls/etcd-client-ca.crt",
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		fmt.Println(err)
	}

	// 客户端配置
	config := clientv3.Config{
		Endpoints:   []string{"https://10.111.7.178:2379"},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	}
	// 建立连接
	client, err := clientv3.New(config)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer client.Close()

	taskConnect := time.NewTicker(2 * time.Second)
	for {
		<-taskConnect.C
		fmt.Println("################# 一轮迭代 ###########")
		// 开始监听path
		watchCh := client.Watch(context.TODO(), path)
		go func() {
			for v := range watchCh {
				for _, e := range v.Events {
					fmt.Printf("type:%v kv:%v  prevKey:%v \n ", e.Type, string(e.Kv.Key), e.PrevKv)
				}

			}
		}()
		run(client)
	}

}

// 一次事务操作
func run(client *clientv3.Client) {
	var kv clientv3.KV
	kv = clientv3.NewKV(client)
	putResp, errCreate := kv.Put(context.TODO(), path, "Hello World!")
	if errCreate != nil {
		xray.ErrMini(errCreate)
	}
	fmt.Println(putResp.Header.Revision)
	if putResp.PrevKv != nil {
		fmt.Printf("prev Value: %s \n CreateRevision : %d \n ModRevision: %d \n Version: %d \n",
			string(putResp.PrevKv.Value), putResp.PrevKv.CreateRevision, putResp.PrevKv.ModRevision, putResp.PrevKv.Version)
	}

	// create
	alpha := 'a'
	for i := 0; i < 26; i++ {
		putResp, errCreate := kv.Put(context.TODO(), path+"/"+string(alpha), "Hello World!"+string(alpha))
		if errCreate != nil {
			xray.ErrMini(errCreate)
		}
		fmt.Println(putResp.Header.Revision)
		if putResp.PrevKv != nil {
			fmt.Printf("prev Value: %s \n CreateRevision : %d \n ModRevision: %d \n Version: %d \n",
				string(putResp.PrevKv.Value), putResp.PrevKv.CreateRevision, putResp.PrevKv.ModRevision, putResp.PrevKv.Version)
		}
		alpha++
	}

	// delete
	for i := 0; i < 26; i++ {
		alpha--
		newPath := path + "/" + string(alpha)
		// get'
		getResp, err := kv.Get(context.TODO(), newPath)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Key is s %s \n Value is %s \n", getResp.Kvs[0].Key, getResp.Kvs[0].Value)

		delResp, errDelete := kv.Delete(context.TODO(), newPath)
		if errDelete != nil {
			xray.ErrMini(errDelete)
		}

		fmt.Println("deleted: ", delResp)
	}
	getResp, err := kv.Get(context.TODO(), path)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Key is s %s \n Value is %s \n", getResp.Kvs[0].Key, getResp.Kvs[0].Value)

	delResp, errDelete := kv.Delete(context.TODO(), path)
	if errDelete != nil {
		xray.ErrMini(errDelete)
	}

	fmt.Println("deleted: ", delResp)
	time.Sleep(time.Second)
}
