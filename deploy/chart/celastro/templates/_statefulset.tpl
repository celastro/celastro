{{/*
The StatefulSet, once per role: the data nodes (`replicas`) and, when
`coordinators.replicas` is set, the coordinators, which hold no shards
and only coordinate. One template, so the two differ only in what a
coordinator is: its name, its headless service, its role label and env,
its resources and its small volume (the catalog is all it keeps).
*/}}
{{- define "celastro.statefulset" -}}
{{- $r := .root -}}
{{- $role := .role -}}
{{- $total := add (int $r.Values.replicas) (int $r.Values.coordinators.replicas) -}}
{{- $name := ternary (printf "%s-coord" (include "celastro.fullname" $r)) (include "celastro.fullname" $r) (eq $role "coordinator") -}}
{{- $svc := $name -}}
{{- $replicas := ternary (int $r.Values.coordinators.replicas) (int $r.Values.replicas) (eq $role "coordinator") -}}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ $name }}
  labels:
    {{- include "celastro.labels" $r | nindent 4 }}
spec:
  # One pod is a database. More are nodes of one cluster: each with its own
  # volume, its own address in the headless service, attaching the others
  # at start. There is no replication between them -- a shard has one
  # holder -- so a pod that is down is its shards down.
  replicas: {{ $replicas }}
  # All at once: every pod attaches the others by retrying until they
  # answer, and none has to wait for a lower ordinal to be ready.
  podManagementPolicy: Parallel
  serviceName: {{ $svc }}
  selector:
    matchLabels:
      {{- include "celastro.selectorLabels" $r | nindent 6 }}
      {{- if eq $role "coordinator" }}
      celastro.io/role: coordinator
      {{- end }}
  template:
    metadata:
      labels:
        {{- include "celastro.selectorLabels" $r | nindent 8 }}
        {{- if eq $role "coordinator" }}
        celastro.io/role: coordinator
        {{- end }}
    spec:
      # The image runs as 65532 and owns /data in the image; a fresh volume
      # is not the image's, so the group is set for it to be writable.
      securityContext:
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        runAsNonRoot: true
      # `serve` handles SIGTERM: it stops accepting, saves, and exits 0 well
      # inside this window.
      terminationGracePeriodSeconds: 30
      containers:
        - name: celastro
          image: "{{ $r.Values.image.repository }}:{{ $r.Values.image.tag | default $r.Chart.AppVersion }}"
          imagePullPolicy: {{ $r.Values.image.pullPolicy }}
          {{- $networked := eq (include "celastro.consoleNetworked" $r) "true" }}
          {{- /* Reachable from outside the pod for a client, for a scrape, or
                 both; the token is what stands in front of it either way. */}}
          {{- $bind := ternary (list "--bind" "0.0.0.0") (list) $networked }}
          {{- if gt $total 1 }}
          args: {{ concat (list "--dir" "/data" "--port" ($r.Values.port | toString) "serve") $bind (list "--shard-bind" (printf "0.0.0.0:%d" (int $r.Values.wire.port))) | toJson }}
          {{- else }}
          args: {{ concat (list "--dir" "/data" "--port" ($r.Values.port | toString) "serve") $bind | toJson }}
          {{- end }}
          env:
            {{- range $k, $v := $r.Values.tuning }}
            - name: {{ $k | quote }}
              value: {{ $v | toString | quote }}
            {{- end }}
            {{- if $r.Values.tls.enabled }}
            - name: CELASTRO_TLS_CERT
              value: /tls/tls.crt
            - name: CELASTRO_TLS_KEY
              value: /tls/tls.key
            - name: CELASTRO_TLS_CA
              value: /tls/ca.crt
            {{- if $r.Values.tls.clientAuth }}
            - name: CELASTRO_TLS_CLIENT_AUTH
              value: {{ $r.Values.tls.clientAuth | quote }}
            {{- end }}
            {{- end }}
            {{- if $r.Values.encryption.existingSecret }}
            - name: CELASTRO_MASTER_KEY_FILE
              value: /keys/master.key
            - name: CELASTRO_KEY_FILE
              value: /keys/KEY
            {{- end }}
            {{- if $networked }}
            - name: CELASTRO_TOKEN
              valueFrom:
                secretKeyRef:
                  name: {{ include "celastro.consoleSecretName" $r }}
                  key: CELASTRO_TOKEN
            {{- end }}
            {{- if gt $total 1 }}
            # The pod's stable name is its address: `tcp://<pod>.<service>:<wire port>`.
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: CELASTRO_NODE
              value: {{ printf "tcp://$(POD_NAME).%s:%d" $svc (int $r.Values.wire.port) | quote }}
            {{- if eq $role "coordinator" }}
            - name: CELASTRO_ROLE
              value: coordinator
            {{- end }}
            - name: CELASTRO_ATTACH
              value: {{ include "celastro.peers" $r | quote }}
            - name: CELASTRO_WIRE_TOKEN
              valueFrom:
                secretKeyRef:
                  name: {{ include "celastro.wireSecretName" $r }}
                  key: CELASTRO_WIRE_TOKEN
            {{- if $r.Values.wire.tokenAlso }}
            - name: CELASTRO_WIRE_TOKEN_ALSO
              value: {{ $r.Values.wire.tokenAlso | quote }}
            {{- end }}
            {{- end }}
            {{- if $r.Values.archive.endpoint }}
            - name: CELASTRO_ARCHIVE_ENDPOINT
              value: {{ $r.Values.archive.endpoint | quote }}
            - name: CELASTRO_ARCHIVE_BUCKET
              value: {{ $r.Values.archive.bucket | quote }}
            - name: CELASTRO_ARCHIVE_PREFIX
              value: {{ $r.Values.archive.prefix | quote }}
            - name: CELASTRO_ARCHIVE_REGION
              value: {{ $r.Values.archive.region | quote }}
            {{- if $r.Values.archive.caSecret }}
            - name: CELASTRO_ARCHIVE_CA
              value: /archive-ca/ca.crt
            {{- end }}
            {{- end }}
            {{- if $r.Values.archive.existingClaim }}
            {{- if not $r.Values.archive.endpoint }}
            - name: CELASTRO_ARCHIVE_DIR
              value: {{ printf "%s/tier" $r.Values.archive.mountPath | quote }}
            {{- end }}
            - name: CELASTRO_BACKUP_DIR
              value: {{ printf "%s/backups" $r.Values.archive.mountPath | quote }}
            {{- end }}
          {{- if $r.Values.archive.endpoint }}
          envFrom:
            - secretRef:
                name: {{ include "celastro.secretName" $r }}
          {{- end }}
          ports:
            - name: console
              containerPort: {{ $r.Values.port }}
            {{- if gt $total 1 }}
            - name: wire
              containerPort: {{ $r.Values.wire.port }}
            {{- end }}
          # The probes ask the console itself, from inside the pod, and the
          # console answers only after it has read its catalog: a process
          # that is up with a database it cannot open is not ready.
          # Ready means able to coordinate: with peers, the node has to have
          # verified every one of them since it started, or a Service would
          # route a statement to a pod that cannot yet reach the shards it
          # does not hold. Liveness asks only whether it serves.
          readinessProbe:
            exec:
              {{- if gt $total 1 }}
              command: ["/celastro", "--port", {{ $r.Values.port | quote }}, "health", "--attached", {{ sub $total 1 | toString | quote }}]
              {{- else }}
              command: ["/celastro", "--port", {{ $r.Values.port | quote }}, "health"]
              {{- end }}
            # The console serves one request at a time, so a probe that lands
            # behind a statement waits for it; the default second was seen
            # to time out during a rolling restart.
            timeoutSeconds: {{ $r.Values.probes.timeoutSeconds }}
            periodSeconds: {{ $r.Values.probes.periodSeconds }}
            failureThreshold: {{ $r.Values.probes.failureThreshold }}
          livenessProbe:
            exec:
              command: ["/celastro", "--port", {{ $r.Values.port | quote }}, "health"]
            initialDelaySeconds: 10
            timeoutSeconds: {{ $r.Values.probes.timeoutSeconds }}
            periodSeconds: {{ $r.Values.probes.periodSeconds }}
            failureThreshold: {{ $r.Values.probes.failureThreshold }}
          volumeMounts:
            - name: data
              mountPath: /data
            {{- if $r.Values.archive.existingClaim }}
            - name: archive
              mountPath: {{ $r.Values.archive.mountPath | quote }}
            {{- end }}
            {{- if $r.Values.tls.enabled }}
            - name: tls
              mountPath: /tls
              readOnly: true
            {{- end }}
            {{- if $r.Values.encryption.existingSecret }}
            - name: keys
              mountPath: /keys
              readOnly: true
            {{- end }}
            {{- if $r.Values.archive.caSecret }}
            - name: archive-ca
              mountPath: /archive-ca
              readOnly: true
            {{- end }}
          {{- with (ternary $r.Values.coordinators.resources $r.Values.resources (eq $role "coordinator")) }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
      {{- if or $r.Values.tls.enabled $r.Values.archive.existingClaim $r.Values.encryption.existingSecret $r.Values.archive.caSecret }}
      volumes:
        {{- if $r.Values.archive.caSecret }}
        - name: archive-ca
          secret:
            secretName: {{ $r.Values.archive.caSecret | quote }}
        {{- end }}
        {{- if $r.Values.tls.enabled }}
        - name: tls
          secret:
            secretName: {{ include "celastro.tlsSecretName" $r }}
        {{- end }}
        {{- if $r.Values.encryption.existingSecret }}
        - name: keys
          secret:
            secretName: {{ $r.Values.encryption.existingSecret | quote }}
        {{- end }}
        {{- if $r.Values.archive.existingClaim }}
        - name: archive
          persistentVolumeClaim:
            claimName: {{ $r.Values.archive.existingClaim | quote }}
        {{- end }}
      {{- end }}
      {{- with $r.Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $r.Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $r.Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        {{- if $r.Values.persistence.storageClass }}
        storageClassName: {{ $r.Values.persistence.storageClass | quote }}
        {{- end }}
        resources:
          requests:
            storage: {{ ternary $r.Values.coordinators.persistence.size $r.Values.persistence.size (eq $role "coordinator") }}
{{- end -}}
