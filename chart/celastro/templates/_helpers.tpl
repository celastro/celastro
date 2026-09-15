{{- define "celastro.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "celastro.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "celastro.labels" -}}
app.kubernetes.io/name: {{ include "celastro.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "celastro.selectorLabels" -}}
app.kubernetes.io/name: {{ include "celastro.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "celastro.wireSecretName" -}}
{{- if .Values.wire.existingSecret -}}
{{- .Values.wire.existingSecret -}}
{{- else -}}
{{- printf "%s-wire" (include "celastro.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "celastro.consoleSecretName" -}}
{{- if .Values.console.existingSecret -}}
{{- .Values.console.existingSecret -}}
{{- else -}}
{{- printf "%s-console" (include "celastro.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Every pod's wire address, comma-separated: what each pod attaches. */}}
{{- define "celastro.peers" -}}
{{- $name := include "celastro.fullname" . -}}
{{- $port := .Values.wire.port -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.replicas) -}}
{{- $peers = append $peers (printf "tcp://%s-%d.%s:%d" $name $i $name (int $port)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{- define "celastro.secretName" -}}
{{- if .Values.archive.existingSecret -}}
{{- .Values.archive.existingSecret -}}
{{- else -}}
{{- printf "%s-archive" (include "celastro.fullname" .) -}}
{{- end -}}
{{- end -}}
