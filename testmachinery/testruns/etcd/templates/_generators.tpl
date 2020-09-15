{{- define "generator" }}
{{ if eq .Values.shoot.cloudprovider "gcp" }}
  - name: generate-provider
    annotations:
      testmachinery.sapcloud.io/system-step: "true"
    definition:
      name: gen-provider-gcp
      config:
      - name: CONTROLPLANE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/controlplane.yaml
      - name: INFRASTRUCTURE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/infra.yaml
      - name: ZONE
        type: env
        value: {{ required "a zone is required for gcp shoots" .Values.shoot.zone }}
{{ else if eq .Values.shoot.cloudprovider "aws" }}
  - name: generate-provider
    annotations:
      testmachinery.sapcloud.io/system-step: "true"
    definition:
      name: gen-provider-aws
      config:
      - name: CONTROLPLANE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/controlplane.yaml
      - name: INFRASTRUCTURE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/infra.yaml
      - name: ZONE
        type: env
        value: {{ required "a zone is required for aws shoots" .Values.shoot.zone }}
{{ else if eq .Values.shoot.cloudprovider "azure" }}
  - name: generate-provider
    annotations:
      testmachinery.sapcloud.io/system-step: "true"
    definition:
      name: gen-provider-azure
      config:
      - name: CONTROLPLANE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/controlplane.yaml
      - name: INFRASTRUCTURE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/infra.yaml
{{ else if eq .Values.shoot.cloudprovider "alicloud" }}
  - name: generate-provider
    annotations:
      testmachinery.sapcloud.io/system-step: "true"
    definition:
      name: gen-provider-alicloud
      config:
      - name: CONTROLPLANE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/controlplane.yaml
      - name: INFRASTRUCTURE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/infra.yaml
      - name: ZONE
        type: env
        value: {{ required "a zone is required for alicloud shoots" .Values.shoot.zone }}
{{ else if eq .Values.shoot.cloudprovider "openstack" }}
  - name: generate-provider
    annotations:
      testmachinery.sapcloud.io/system-step: "true"
    definition:
      name: gen-provider-openstack
      config:
      - name: CONTROLPLANE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/controlplane.yaml
      - name: INFRASTRUCTURE_PROVIDER_CONFIG_FILEPATH
        type: env
        value: /tmp/tm/shared/generators/infra.yaml
      - name: ZONE
        type: env
        value: {{ required "a zone is required for openstack shoots" .Values.shoot.zone }}
      - name: FLOATING_POOL_NAME
        type: env
        value: {{ required "floating pool name is required for openstack shoots" .Values.shoot.floatingPoolName }}
      - name: LOADBALANCER_PROVIDER
        type: env
        value: {{ required "a loadbalancer provider name is required for openstack shoots" .Values.shoot.loadbalancerProvider }}
{{ end }}
{{- end }}

{{- define "config-overwrites" }}
      {{ if .Values.shoot.infrastructureConfig }}
      - name: INFRASTRUCTURE_PROVIDER_CONFIG_FILEPATH
        type: file
        path: /tmp/tm/shared/generators/infra.yaml
        value: {{ .Values.shoot.infrastructureConfig }}
      {{ end }}
      {{ if .Values.shoot.controlplaneConfig }}
      - name: CONTROLPLANE_PROVIDER_CONFIG_FILEPATH
        type: file
        path: /tmp/tm/shared/generators/controlplane.yaml
        value: {{ .Values.shoot.controlplaneConfig }}
      {{ end }}
{{- end }}