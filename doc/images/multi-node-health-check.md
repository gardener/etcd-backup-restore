title Multi-node etcd health check

participant backup-1
participant backup-2
participant backup-3
participant etcd-druid
participant kube-apiserver
participant etcd-client-service-external

note over etcd-client-service-external:apiVersion: v1\nkind: Service\nmetadata:\n  name: etcd-client-service-external\nspec:\n <color:#purple> selector:\n    app: etcd </color> \n  ports:\n    - name: http\n      protocol: TCP\n      port: 2379\n      targetPort: 2379

kube-apiserver -> etcd-client-service-external : get/put key
kube-apiserver <#red-- etcd-client-service-external :<color:#red> response 404

loop Atleast one unhealthy backup process?
etcd-druid->backup-1:IsHealthy?
etcd-druid<--backup-1:backup-1-health
etcd-druid->backup-2:IsHealthy?
etcd-druid<--backup-2:backup-2-health
etcd-druid->backup-3:IsHealthy?
etcd-druid<--backup-3:backup-3-health
end

etcd-druid-> etcd-client-service-external:update label {"scope":"public"}
note over etcd-client-service-external :apiVersion: v1\nkind: Service\nmetadata:\n  name: etcd-client-service-external\nspec:\n <color:#blue> selector:\n    app: etcd\n    scope: public</color>\n  ports:\n    - name: http\n      protocol: TCP\n      port: 2379\n      targetPort: 2379
etcd-druid<-- etcd-client-service-external: updated service selector

kube-apiserver -> etcd-client-service-external : get/put key
kube-apiserver <#green-- etcd-client-service-external : <color:#green>response 200

