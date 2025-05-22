import boto3
import lithops
import re

iterdata = ['s3://task2-3-texts/text1.csv', 's3://task2-3-texts/text2.csv', 's3://task2-3-texts/text3.csv']
insults = ['tonto', 'idiota', 'mierda', 'gilipollas', 'mamabicho', 'cabron']

def my_map_function(obj):
    s3_client = boto3.client('s3')
    
    bucket = obj.bucket
    key = obj.key
    output = key.split("/")[-1] 
    nom_output = f'censored_{output}'
    
    # Leer todo el contenido del archivo en memoria
    data = obj.data_stream.read()
    text_censurat = data.decode('utf-8')
    
    arxiu_temporal = f'/tmp/censored_{output}'

    count = 0

    # Procesar el texto con regex para censurar insultos
    for insult in insults:
        pattern = re.compile(r'\b{}\b'.format(re.escape(insult)), flags=re.IGNORECASE)
        matches = len(pattern.findall(text_censurat))
        count += matches
        text_censurat = pattern.sub('CENSORED', text_censurat)
    
    # Guardar el texto censurado en un archivo temporal
    with open(arxiu_temporal, 'w', encoding='utf-8') as fitxer_output:
        fitxer_output.write(text_censurat)
    
    # Subir el archivo censurado a S3
    s3_client.upload_file(
        Filename=arxiu_temporal,
        Bucket=bucket,
        Key=nom_output
    )
    
    return count



def my_reduce_function(results):
    return sum(results)


if __name__ == "__main__":
    fexec = lithops.FunctionExecutor(
            backend='aws_lambda',
            runtime_memory=4096,   
            runtime_timeout=300,   
            log_level='INFO'       
        )    
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function)
    result = fexec.get_result()
    print(f'insults censurats: {result}')