from qdrant_client import QdrantClient
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from qdrant_client import  models
from concurrent.futures import ThreadPoolExecutor, as_completed

client = QdrantClient("http://localhost:6333")
# client.create_collection(
#     collection_name="words",
#     vectors_config=models.VectorParams(
#         size=100,
#         distance=models.Distance.COSINE
#     ),
#     hnsw_config={
#         "m": 32,
#         "ef_construct": 20,
#         "full_scan_threshold": 10000
#     },
#     on_disk_payload=False  # Lưu payload trên đĩa
# )
client.create_collection(
    collection_name="words",
    vectors_config=models.VectorParams(
        size=100,  
        distance=models.Distance.COSINE  
    ),
    quantization_config=models.BinaryQuantization(
        binary=models.BinaryQuantizationConfig(always_ram=False), 
    ),
    hnsw_config=models.HnswConfigDiff(
        m=32, 
        ef_construct=100,  
        full_scan_threshold=10000,  
        on_disk=True  
    ),
    optimizers_config=models.OptimizersConfigDiff(
        default_segment_number=5,  
        indexing_threshold=0,  
    ),
    on_disk_payload=True  
)

client.create_payload_index(
    collection_name="words",
    field_name="word",
    field_type="keyword"
)

def read_stopwords(file_path):
    """Đọc danh sách từ dừng từ file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        stopwords = set(line.strip() for line in f)  
    return stopwords

def read_vectors(file_path, stopwords):
    """Đọc file chứa từ và vector, bỏ dòng đầu tiên và loại bỏ từ dừng"""
    vectors = []
    with open(file_path, 'r', encoding='utf-8') as f:
        next(f)  
        for line in f:
            parts = line.split()
            word = parts[0]  
            if word not in stopwords:  
                try:
                    if word == 'Industries':
                        print(1)
                    vector = list(map(float, parts[1:]))  
                    if len(vector) == 100:  
                        vectors.append((word, vector)) 
                    else:
                        print(f"Vector không đúng số chiều: {word}")
                except ValueError:
                    print(f"Không thể chuyển đổi vector: {word}")
    return vectors
stopwords_file = 'vietnamese-stopwords-dash.txt'
vector_file = 'word2vec_vi_words_100dims.txt'
stopwords = read_stopwords(stopwords_file)

vectors = read_vectors(vector_file, stopwords)

# client.recreate_collection(
#     collection_name="words",
#     vectors_config={
#         "size": len(vectors[0][1]),  
#         "distance": "Cosine" 
#     }
# )

# def batch_upsert(vectors_batch, start_index):
#     points = [
#         {
#             "id": start_index + i, 
#             "vector": vector,
#             "payload": {"word": word}
#         }
#         for i, (word, vector) in enumerate(vectors_batch)
#     ]
#     client.upsert(
#         collection_name="words",
#         points=points
#     )

# batch_size = 100  
# start_index = 0  
# print("bắt đầu")
# vector_batches = [vectors[i:i + batch_size] for i in range(0, len(vectors), batch_size)]

# with ThreadPoolExecutor(max_workers=8) as executor:
#     for batch in vector_batches:
#         executor.submit(batch_upsert, batch, start_index)
#         start_index += len(batch)  











def batch_upsert(vectors_batch, start_index):
    points = [
        {
            "id": start_index + i,  # Sử dụng index tính trước
            "vector": vector,
            "payload": {"word": word}
        }
        for i, (word, vector) in enumerate(vectors_batch)
    ]
    client.upsert(
        collection_name="words",
        points=points
    )

batch_size = 100
start_index = 0
print("Bắt đầu thêm dữ liệu...")

# Chia dữ liệu thành các lô (batches)
vector_batches = [vectors[i:i + batch_size] for i in range(0, len(vectors), batch_size)]

# Sử dụng ThreadPoolExecutor với số lượng worker hợp lý
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = []
    for batch in vector_batches:
        # Gửi từng batch vào trong thread và giữ tương ứng với start_index
        futures.append(executor.submit(batch_upsert, batch, start_index))
        start_index += len(batch)

    for future in as_completed(futures):
        result = future.result()  # Có thể kiểm tra lỗi nếu cần
        print("Một batch đã hoàn thành.")




