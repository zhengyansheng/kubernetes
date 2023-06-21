#!/usr/bin/env bash

./kube-apiserver \
--advertise-address=10.112.0.20 \
--allow-privileged=true \
--authorization-mode=Node,RBAC \
--client-ca-file=/Users/zhengyansheng/pki/ca.crt \
--enable-admission-plugins=NodeRestriction \
--enable-bootstrap-token-auth=true \
--etcd-cafile=/Users/zhengyansheng/pki/etcd/ca.crt \
--etcd-certfile=/Users/zhengyansheng/pki/apiserver-etcd-client.crt \
--etcd-keyfile=/Users/zhengyansheng/pki/apiserver-etcd-client.key \
--etcd-servers=https://10.112.0.20:2379 \
--kubelet-client-certificate=/Users/zhengyansheng/pki/apiserver-kubelet-client.crt \
--kubelet-client-key=/Users/zhengyansheng/pki/apiserver-kubelet-client.key \
--kubelet-preferred-address-types=InternalIP,ExternalIP,Hostname \
--proxy-client-cert-file=/Users/zhengyansheng/pki/front-proxy-client.crt \
--proxy-client-key-file=/Users/zhengyansheng/pki/front-proxy-client.key \
--requestheader-allowed-names=front-proxy-client \
--requestheader-client-ca-file=/Users/zhengyansheng/pki/front-proxy-ca.crt \
--requestheader-extra-headers-prefix=X-Remote-Extra- \
--requestheader-group-headers=X-Remote-Group \
--requestheader-username-headers=X-Remote-User \
--secure-port=6443 \
--service-account-issuer=https://kubernetes.default.svc.cluster.local \
--service-account-key-file=/Users/zhengyansheng/pki/sa.pub \
--service-account-signing-key-file=/Users/zhengyansheng/pki/sa.key \
--service-cluster-ip-range=10.96.0.0/12 \
--tls-cert-file=/Users/zhengyansheng/pki/apiserver.crt \
--tls-private-key-file=/Users/zhengyansheng/pki/apiserver.key
#--v=4