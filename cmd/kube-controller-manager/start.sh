./kube-controller-manager \
--authentication-kubeconfig=./etc/kubernetes/controller-manager.conf \
--authorization-kubeconfig=./etc/kubernetes/controller-manager.conf \
--bind-address=127.0.0.1 \
--client-ca-file=./etc/kubernetes/pki/ca.crt \
--cluster-name=kubernetes \
--cluster-signing-cert-file=./etc/kubernetes/pki/ca.crt \
--cluster-signing-key-file=./etc/kubernetes/pki/ca.key \
--controllers=*,bootstrapsigner,tokencleaner \
--kubeconfig=./etc/config \
--leader-elect=true \
--requestheader-client-ca-file=./etc/kubernetes/pki/front-proxy-ca.crt \
--root-ca-file=./etc/kubernetes/pki/ca.crt \
--service-account-private-key-file=./etc/kubernetes/pki/sa.key \
--use-service-account-credentials=true



# CGO_ENABLED=0 GOOS=linux go build -o kube-controller-manager controller-manager.go