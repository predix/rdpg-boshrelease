#!/bin/bash

set -e
set -x

cf --version
cf api --skip-ssl-validation ${cf_api_url}
cf auth ${cf_admin_username} ${cf_admin_password}

cf delete-service-broker -f $broker_name

cf create-service-broker ${broker_name} ${broker_username} ${broker_password} ${broker_url} || \
  cf update-service-broker ${broker_name} ${broker_username} ${broker_password} ${broker_url}


found=false
while read -r line
do
	if [[ ${found} == "true" ]]
	then
		service_name=( $(echo ${line} | awk '{print $1}') )
		case ${service_name} in 
			(service) 
				continue 
				;;
			(*)
        if [[ ${service_name} == "" ]]
        then break
        fi
				echo "Enabling Service Access For ${service_name}"
				cf enable-service-access ${service_name}
				;;
		esac
	else
		case ${line} in
			(*broker:\ ${broker_name}*)
				found=true
				;;
		esac
	fi
done <<< "$(cf service-access)"

echo "Service Access:"
cf service-access

