{{- define "sluice.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "sluice.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}-{{ .Chart.Name }}
{{- end }}

{{- define "sluice.labels" -}}
app.kubernetes.io/name: {{ include "sluice.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "sluice.worker-labels" -}}
app.kubernetes.io/name: {{ include "sluice.name" . }}-worker
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: worker
{{- end }}

{{- define "sluice.headless-svc" -}}
{{- include "sluice.fullname" . }}-headless
{{- end }}
