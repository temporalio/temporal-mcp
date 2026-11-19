{{- define "temporal-mcp.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "temporal-mcp.listenPort" -}}
{{- $config := required "config is required: pass the server's config file with --set-file config=config.yaml. See docs/config.md in https://github.com/temporalio/temporal-mcp" .Values.config | fromYaml -}}
{{- required "config must be YAML that sets listen, the address the server serves MCP on" $config.listen | splitList ":" | last -}}
{{- end }}

{{- define "temporal-mcp.requireStrings" -}}
{{- $path := index . 0 }}{{ $value := index . 1 }}
{{- if kindIs "map" $value }}{{ range $k, $v := $value }}{{ include "temporal-mcp.requireStrings" (list (printf "%s: %s" $path $k) $v) }}{{ end }}
{{- else if kindIs "slice" $value }}{{ range $i, $v := $value }}{{ include "temporal-mcp.requireStrings" (list (printf "%s[%d]" $path $i) $v) }}{{ end }}
{{- else if not (or (kindIs "string" $value) (kindIs "invalid" $value)) }}{{ fail (printf "%s is not a YAML string, so with watchConfig the pods might not restart when it changes; quote it" $path) }}
{{- end }}
{{- end }}
