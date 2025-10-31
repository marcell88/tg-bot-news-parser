# Тестовый скрипт для проверки модели
from sentence_transformers import SentenceTransformer
import numpy as np

def test_model_quality():
    model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')
    
    # Тестовые примеры на русском
    texts = [
        "Путин встретился с лидерами стран ЕС",
        "Президент России провел переговоры с Европейским союзом", 
        "Котики и собачки - милые домашние животные",
        "Футбольный матч между сборными России и Бразилии"
    ]
    
    embeddings = model.encode(texts)
    print(f"Размерность векторов: {embeddings.shape[1]}")  # Должно быть 768
    
    # Проверяем сходство
    from sklearn.metrics.pairwise import cosine_similarity
    similarities = cosine_similarity([embeddings[0]], embeddings[1:])
    
    print("Сходство с другими текстами:")
    print(f"Похожий текст: {similarities[0][0]:.3f}")  # Должен быть высокий
    print(f"Непохожий текст 1: {similarities[0][1]:.3f}")  # Должен быть низкий
    print(f"Непохожий текст 2: {similarities[0][2]:.3f}")  # Должен быть низкий

if __name__ == "__main__":
    test_model_quality()