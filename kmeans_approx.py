# kmeans_approx.py

import numpy as np
from sklearn.metrics import pairwise_distances_argmin


class KMeansApprox:
    """
    基于质心的 KMeans 近似模型
    用于从已有标签重建聚类模型
    """

    def __init__(self, centers, scaler=None):
        """
        初始化模型

        Args:
            centers: 聚类中心点 (k x n_features)
            scaler: StandardScaler 对象（可选）
        """
        self.centers = centers
        self.scaler = scaler
        self.n_clusters = len(centers)

    def predict(self, X_new):
        """
        预测新数据的聚类标签

        Args:
            X_new: 新数据 (n_samples x n_features)

        Returns:
            labels: 聚类标签 (n_samples,)
        """
        # 如果有 scaler，先标准化
        if self.scaler is not None:
            X_new_scaled = self.scaler.transform(X_new)
        else:
            X_new_scaled = X_new

        # 分配到最近的质心
        labels = pairwise_distances_argmin(X_new_scaled, self.centers)

        return labels

    def __repr__(self):
        return f"KMeansApprox(n_clusters={self.n_clusters}, has_scaler={self.scaler is not None})"