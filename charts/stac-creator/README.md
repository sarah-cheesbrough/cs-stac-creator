stac-creator
============
A Helm chart for Kubernetes

Current chart version is `0.1.4`

Source code can be found [here](https://github.com/SatelliteApplicationsCatapult/cs-stac-creator)



## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| autoscaling.enabled | bool | `false` |  |
| autoscaling.maxReplicas | int | `100` |  |
| autoscaling.minReplicas | int | `1` |  |
| autoscaling.targetCPUUtilizationPercentage | int | `80` |  |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"satapps/cs-stac-creator"` |  |
| imagePullSecrets | list | `[]` |  |
| nameOverride | string | `""` |  |
| nats.hostname | string | `"nats"` |  |
| nodeSelector | object | `{}` |  |
| podAnnotations | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| replicaCount | int | `1` |  |
| resources | object | `{}` |  |
| s3.accessKeyId | string | `"secret"` |  |
| s3.endpoint | string | `"https://s3-uk-1.sa-catapult.co.uk"` |  |
| s3.secretAccessKey | string | `"secret"` |  |
| securityContext | object | `{}` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| tolerations | list | `[]` |  |
