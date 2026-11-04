{{- define "temporal-mcp.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "temporal-mcp.listenPort" -}}
{{- $config := required "config is required: pass the server's config file with --set-file config=config.yaml. See docs/config.md in https://github.com/temporalio/temporal-mcp" .Values.config | fromYaml -}}
{{- required "config must be YAML that sets listen, the address the server serves MCP on" $config.listen | splitList ":" | last -}}
{{- end }}
