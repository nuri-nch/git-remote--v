# -------------------------------
# Script PowerShell: Spark + Airflow
# -------------------------------

# 1️⃣ Configura tu ruta a la llave
$KEY_PATH = "C:\Users\chala\Desktop\PIM4\SparkKeyV3.pem"

# 2️⃣ Terminar instancias antiguas
Write-Host "Buscando instancias viejas..."
$OLD_INSTANCES = aws ec2 describe-instances `
    --filters "Name=tag:Name,Values=SparkComputeNode,AirflowNode" `
    --query "Reservations[*].Instances[*].InstanceId" --output text

if ($OLD_INSTANCES) {
    Write-Host "Terminando instancias viejas: $OLD_INSTANCES"
    aws ec2 terminate-instances --instance-ids $OLD_INSTANCES
    Start-Sleep -Seconds 15
} else {
    Write-Host "No hay instancias viejas para terminar."
}

# 3️⃣ Obtener la última AMI de Amazon Linux 2 en us-east-2
Write-Host "Obteniendo AMI válida..."
$AMI = aws ec2 describe-images `
    --owners amazon `
    --filters "Name=name,Values=amzn2-ami-hvm-*-x86_64-gp2" "Name=state,Values=available" `
    --query "Images | sort_by(@, &CreationDate) | [-1].ImageId" `
    --region us-east-2 --output text
Write-Host "AMI seleccionada: $AMI"

# 4️⃣ Lanzar SERVIDOR SPARK
Write-Host "Lanzando instancia Spark..."
$SPARK_EC2 = aws ec2 run-instances `
    --image-id $AMI `
    --count 1 `
    --instance-type t3.medium `
    --key-name "SparkKeyV3" `
    --security-group-ids sg-0dcb110700fba2b73 `
    --iam-instance-profile Name="SparkS3Profile" `
    --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled" `
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=16,VolumeType=gp3}' `
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=SparkComputeNode}]' `
    --query "Instances[0].InstanceId" --output text
Write-Host "Instancia Spark creada: $SPARK_EC2"

# 5️⃣ Lanzar SERVIDOR AIRFLOW
Write-Host "Lanzando instancia Airflow..."
$AIRFLOW_EC2 = aws ec2 run-instances `
    --image-id $AMI `
    --count 1 `
    --instance-type t3.medium `
    --key-name "SparkKeyV3" `
    --security-group-ids sg-0dcb110700fba2b73 `
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=16,VolumeType=gp3}' `
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=AirflowNode}]' `
    --query "Instances[0].InstanceId" --output text
Write-Host "Instancia Airflow creada: $AIRFLOW_EC2"

# 6️⃣ Esperar 20 segundos para que AWS asigne IPs
Write-Host "Esperando 20 segundos para que las instancias se inicialicen..."
Start-Sleep -Seconds 20

# 7️⃣ Mostrar IPs públicas y privadas de ambas instancias
Write-Host "Mostrando IPs de las instancias..."
aws ec2 describe-instances `
    --filters "Name=instance-state-name,Values=running" "Name=tag:Name,Values=SparkComputeNode,AirflowNode" `
    --query "Reservations[*].Instances[*].{Nombre:Tags[?Key=='Name']|[0].Value, IP_Publica:PublicIpAddress, IP_Privada:PrivateIpAddress}" `
    --output table