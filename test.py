# improved_embedding_test.py
import numpy as np
from sentence_transformers import SentenceTransformer
import logging
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances
from scipy.spatial.distance import jaccard, cityblock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AdvancedTextComparator:
    def __init__(self):
        self.model_name = 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'
        logging.info(f"Загрузка модели: {self.model_name}")
        self.model = SentenceTransformer(self.model_name)
        logging.info("Модель загружена успешно")
    
    def normalize_vector(self, vec):
        """Нормализует вектор к единичной длине"""
        norm = np.linalg.norm(vec)
        if norm == 0:
            return vec
        return vec / norm
    
    def cosine_similarity(self, vec1, vec2):
        """Косинусное сходство (оригинальное)"""
        return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
    
    def adjusted_cosine_similarity(self, vec1, vec2):
        """Скорректированное косинусное сходство с порогом"""
        similarity = self.cosine_similarity(vec1, vec2)
        # Понижаем оценку для слишком высоких значений
        if similarity > 0.9:
            return 0.9 + (similarity - 0.9) * 0.1  # Сжимаем верхний диапазон
        return similarity
    
    def euclidean_similarity(self, vec1, vec2):
        """Евклидово сходство (оригинальное)"""
        distance = np.linalg.norm(vec1 - vec2)
        return 1.0 / (1.0 + distance)
    
    def normalized_euclidean_similarity(self, vec1, vec2):
        """Евклидово сходство с нормализованными векторами"""
        vec1_norm = self.normalize_vector(vec1)
        vec2_norm = self.normalize_vector(vec2)
        distance = np.linalg.norm(vec1_norm - vec2_norm)
        return 1.0 - (distance / 2.0)  # Нормализуем к [0, 1]
    
    def manhattan_similarity(self, vec1, vec2):
        """Манхэттенское расстояние (L1 норма)"""
        distance = np.sum(np.abs(vec1 - vec2))
        return 1.0 / (1.0 + distance / len(vec1))  # Нормализуем
    
    def dot_product_similarity(self, vec1, vec2):
        """Скалярное произведение с нормализацией"""
        vec1_norm = self.normalize_vector(vec1)
        vec2_norm = self.normalize_vector(vec2)
        return np.dot(vec1_norm, vec2_norm)
    
    def combined_similarity(self, vec1, vec2):
        """Комбинированная метрика (среднее косинуса и нормализованного евклида)"""
        cosine = self.adjusted_cosine_similarity(vec1, vec2)
        euclidean = self.normalized_euclidean_similarity(vec1, vec2)
        return (cosine + euclidean) / 2.0
    
    def analyze_similarity_pattern(self, text1, text2, vec1, vec2):
        """Анализирует паттерн различий между векторами"""
        diff = np.abs(vec1 - vec2)
        max_diff_indices = np.argsort(diff)[-5:]  # 5 самых разных измерений
        avg_diff = np.mean(diff)
        
        print(f"   Анализ различий:")
        print(f"   - Средняя разница: {avg_diff:.4f}")
        print(f"   - Макс различия в измерениях: {max_diff_indices}")
        print(f"   - Длина вектора 1: {np.linalg.norm(vec1):.4f}")
        print(f"   - Длина вектора 2: {np.linalg.norm(vec2):.4f}")
    
    def compare_texts_comprehensive(self, text1, text2):
        """Всестороннее сравнение текстов"""
        print(f"\n🔍 Сравнение текстов:")
        print(f"   Текст 1: '{text1}'")
        print(f"   Текст 2: '{text2}'")
        
        # Создаем эмбеддинги
        embeddings = self.model.encode([text1, text2])
        vec1, vec2 = embeddings[0], embeddings[1]
        
        print(f"   Размерность векторов: {vec1.shape}")
        
        # Вычисляем все метрики
        metrics = {
            'Косинусное': self.cosine_similarity(vec1, vec2),
            'Скорректированное косинусное': self.adjusted_cosine_similarity(vec1, vec2),
            'Евклидово': self.euclidean_similarity(vec1, vec2),
            'Нормализованное евклидово': self.normalized_euclidean_similarity(vec1, vec2),
            'Манхэттенское': self.manhattan_similarity(vec1, vec2),
            'Скалярное произведение': self.dot_product_similarity(vec1, vec2),
            'Комбинированное': self.combined_similarity(vec1, vec2)
        }
        
        # Анализ различий
        self.analyze_similarity_pattern(text1, text2, vec1, vec2)
        
        # Выводим результаты
        print(f"\n   📊 Результаты сравнения:")
        for name, value in metrics.items():
            level = self._get_similarity_level(value)
            print(f"   - {name:<25}: {value:.4f} ({level})")
        
        return metrics, vec1, vec2
    
    def _get_similarity_level(self, similarity):
        """Определяет уровень схожести"""
        if similarity > 0.85:
            return "ОЧЕНЬ ВЫСОКАЯ 🎯"
        elif similarity > 0.7:
            return "ВЫСОКАЯ ✅"
        elif similarity > 0.5:
            return "СРЕДНЯЯ ⚠️"
        elif similarity > 0.3:
            return "НИЗКАЯ 📉"
        else:
            return "ОЧЕНЬ НИЗКАЯ ❌"
    
    def test_multiple_examples(self):
        """Тестирует на нескольких примерах"""
        examples = [
            ("Оплата истребителей Gripen", "Оплата истребителей"),
            ("Путин и Зеленский", "Байден и Трамп"),
            ("Технологии искусственного интеллекта", "Машинное обучение"),
            ("Яблоки и апельсины", "Фрукты и овощи"),
            ("Разработка программного обеспечения", "Кодирование на Python")
        ]
        
        print("\n" + "="*70)
        print("ТЕСТИРОВАНИЕ НА РАЗНЫХ ПРИМЕРАХ")
        print("="*70)
        
        for text1, text2 in examples:
            print(f"\nПример: '{text1}' vs '{text2}'")
            metrics, _, _ = self.compare_texts_comprehensive(text1, text2)
            
            # Рекомендация
            best_metric = max(metrics.items(), key=lambda x: x[1])
            print(f"   💡 Рекомендуемая метрика: {best_metric[0]} ({best_metric[1]:.4f})")

def main():
    # Константы для сравнения
    TEXT1 = "Экономика"
    TEXT2 = "Политика"
    
    # Инициализация компаратора
    comparator = AdvancedTextComparator()
    
    print("="*70)
    print("УЛУЧШЕННОЕ СРАВНЕНИЕ ТЕКСТОВ С РАЗНЫМИ МЕТРИКАМИ")
    print("="*70)
    
    # Основное сравнение
    metrics, vec1, vec2 = comparator.compare_texts_comprehensive(TEXT1, TEXT2)
    
    # Тестируем на разных примерах
    comparator.test_multiple_examples()
    
    print("\n" + "="*70)
    print("РЕКОМЕНДАЦИИ:")
    print("="*70)
    print("1. Для похожих текстов: 'Комбинированная' или 'Нормализованное евклидово'")
    print("2. Для разных текстов: 'Скорректированное косинусное'")
    print("3. Избегайте чистого косинуса для очень похожих текстов")
    print("4. 'Комбинированная' метрика - наиболее сбалансированная")

if __name__ == "__main__":
    main()