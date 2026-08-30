{{- define "vayobyzh.name" -}}
vayobyzh
{{- end }}

{{- define "vayobyzh.fullname" -}}
{{- if contains (include "vayobyzh.name" .) .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "vayobyzh.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "vayobyzh.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "vayobyzh.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: vayobyzh
{{- end }}

{{- define "vayobyzh.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vayobyzh.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "vayobyzh.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "vayobyzh.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "vayobyzh.secretName" -}}
{{- if .Values.secrets.create }}
{{- printf "%s-secrets" (include "vayobyzh.fullname" .) }}
{{- else }}
{{- required "secrets.existingSecret is required when secrets.create=false" .Values.secrets.existingSecret }}
{{- end }}
{{- end }}

{{- define "vayobyzh.apiImage" -}}
{{- if .Values.images.api.digest -}}
{{ printf "%s@%s" .Values.images.api.repository .Values.images.api.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.images.api.repository .Values.images.api.tag }}
{{- end -}}
{{- end }}

{{- define "vayobyzh.webImage" -}}
{{- if .Values.images.web.digest -}}
{{ printf "%s@%s" .Values.images.web.repository .Values.images.web.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.images.web.repository .Values.images.web.tag }}
{{- end -}}
{{- end }}
