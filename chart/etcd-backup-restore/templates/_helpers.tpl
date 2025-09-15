{{- define "gcs.credentialsConfig" -}}
{{- if .Values.backup.tokenAuth.enabled -}}
{{- .Values.backup.gcs.credentialsConfig | toYaml }}
credential_source:
  file: /var/.gcp/token
  format:
    type: text
{{- end -}}
{{- end -}}
