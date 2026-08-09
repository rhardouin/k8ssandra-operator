{{/* Resolve the cass-operator subchart service account from its supported name overrides. */}}
{{- define "k8ssandra-operator.cassOperatorServiceAccountName" -}}
{{- $values := index .Values "cass-operator" | default dict -}}
{{- $serviceAccount := index $values "serviceAccount" | default dict -}}
{{- $defaultName := include "common.names.dependency.fullname" (dict "chartName" "cass-operator" "chartValues" $values "context" .) -}}
{{- default $defaultName (index $serviceAccount "name") -}}
{{- end -}}
