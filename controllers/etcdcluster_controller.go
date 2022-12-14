/*
Copyright 2022 yytyyt.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha1 "github.com/yytyyt/etcd-operator/api/v1alpha1"
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=etcd.ydzs.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=etcd.ydzs.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *EtcdClusterReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	logger := r.Log.WithValues("etcdcluster", req.NamespacedName)

	// 首先获取 EtcdCluster 实例
	// r.client.Get 不会从indexer获取数据 直接从ApiServer获取数据
	// r.Get 从indexer获取数据
	var etcdCluster etcdv1alpha1.EtcdCluster
	if err := r.Get(ctx, req.NamespacedName, &etcdCluster); err != nil {
		// 如果etcdCluster 是被删除的 那么我们应该忽略掉
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 已经获取到了 etcdCluster实例
	// 创建或者更新对应的StatefulSet 以及 Headless SVC 对象
	// CreateOrUpdate
	// 调谐：观察当前的状态和期望的状态进行对比

	// CreateOrUpdate Service
	var svc corev1.Service
	svc.Name = etcdCluster.Name
	svc.Namespace = etcdCluster.Namespace

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		operationResult, err := ctrl.CreateOrUpdate(ctx, r.Client, &svc, func() error {
			// 调谐的函数必须在这里面实现  实际上就是去拼装我们的 Service
			MutateHeadlessSvc(&etcdCluster, &svc)
			return ctrl.SetControllerReference(&etcdCluster, &svc, r.Scheme)
		})
		logger.Info("CreateOrUpdate Result", "Service", operationResult)
		return err
	}); err != nil {
		return ctrl.Result{}, err
	}

	// CreateOrUpdate StatefulSet
	var sts appsv1.StatefulSet
	sts.Name = etcdCluster.Name
	sts.Namespace = etcdCluster.Namespace
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		operationResult, err := ctrl.CreateOrUpdate(ctx, r.Client, &sts, func() error {
			// 调谐的函数必须在这里面实现  实际上就是去拼装我们的 StatefulSet
			MutateStatefulSet(&etcdCluster, &sts)
			return ctrl.SetControllerReference(&etcdCluster, &sts, r.Scheme)
		})
		logger.Info("CreateOrUpdate Result", "StatefulSet", operationResult)
		return err
	}); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
