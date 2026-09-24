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

{{- define "celastro.tlsSecretName" -}}
{{- if .Values.tls.existingSecret -}}
{{- .Values.tls.existingSecret -}}
{{- else -}}
{{- printf "%s-tls" (include "celastro.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Every name a pod's certificate has to carry: the pods, the headless
     Service, the console Service, and localhost for the probe. */}}
{{- define "celastro.tlsNames" -}}
{{- $name := include "celastro.fullname" . -}}
{{- $names := list "localhost" $name (printf "%s-console" $name) (printf "%s.%s.svc" $name .Release.Namespace) (printf "%s-console.%s.svc" $name .Release.Namespace) -}}
{{- range $i := until (int .Values.replicas) -}}
{{- $names = append $names (printf "%s-%d.%s" $name $i $name) -}}
{{- $names = append $names (printf "%s-%d.%s.%s.svc" $name $i $name $.Release.Namespace) -}}
{{- end -}}
{{- join "," $names -}}
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
{{- $coord := printf "%s-coord" $name -}}
{{- range $i := until (int .Values.coordinators.replicas) -}}
{{- $peers = append $peers (printf "tcp://%s-%d.%s:%d" $coord $i $coord (int $port)) -}}
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

{{/* Whether the console listens on the pod's network rather than on
     127.0.0.1 inside it. `console.expose` asks for that so clients can
     reach it through the console Service; `monitoring.enabled` needs the
     same thing for a different reason -- a scrape comes from another pod --
     and both then want the token, since a network console never runs
     without one. */}}
{{- define "celastro.consoleNetworked" -}}
{{- if or .Values.console.expose .Values.monitoring.enabled -}}true{{- end -}}
{{- end -}}
