# -*- coding: utf-8 -*-
"""
Created on 2023-09-17 22:27:54

@author: guoshuangpeng@le.com
"""
import json
import os
import paddlets
from paddlets.datasets.repository import get_dataset, dataset_list
from paddlets.models.forecasting import MLPRegressor
from paddlets.automl.autots import AutoTS


print(paddlets.__version__)

# 目前公开可用来测试的数据集
print(f"built-in datasets: {dataset_list()}")

# 天气数据
dataset = get_dataset('UNI_WTH')
print(type(dataset))

# 查看数据
# 图表
# dataset.plot()
# 统计信息
# dataset.summary()


# 拆分训练集、测试集、验证集
val_test_dataset, train_dataset = dataset.split(0.1)
val_dataset, test_dataset = val_test_dataset.split(0.5)

# 写入文件
# with open("train_dataset.json", "w") as f:
#     f.writelines(train_dataset.to_json())
#
# with open("val_dataset.json", "w") as f:
#     f.writelines(val_dataset.to_json())
#
# with open("test_dataset.json", "w") as f:
#     f.writelines(test_dataset.to_json())

# 定义多层感知器训练模型
best_estimator = MLPRegressor(
    in_chunk_len=7 * 24,    # 输入时序窗口的大小
    out_chunk_len=24,       # 输出时序窗口的大小
    max_epochs=300,         # 最大迭代步数
    use_bn=True,
    batch_size=128,
    hidden_config=[64, 64, 64],
    patience=35,
    optimizer_params={
        "learning_rate": 0.006752222304089357
    }
)

# 训练
best_estimator.fit(train_dataset, test_dataset)

# 自动调优模型
# autots_model = AutoTS(MLPRegressor, 7 * 24, 24)
#
# os.environ["CUDA_VISIBLE_DEVICES"] = "0,1,2,3,4,5,6,7"
# best_model = autots_model.fit(train_dataset,
#                               test_dataset,
#                               cpu_resource=1,
#                               gpu_resource=1)
# best_model.save("./best_model")
#
# # 保存最优参数
# best_param = autots_model.best_param
# with open("best_param.json", "w") as f:
#     f.writelines(json.dumps(best_param))
#
# # 保存最优模型
# best_estimator = autots_model.best_estimator()
# best_estimator.save("best_estimator")

# 模型验证
subset_test_pred_dataset = best_estimator.predict(val_dataset)
subset_test_pred_dataset.plot().get_figure().savefig('subset_test_pred_dataset.png')

# 测试数据 真实数据对比
subset_test_dataset, _ = test_dataset.split(len(subset_test_pred_dataset.target))
subset_test_dataset.plot(add_data=subset_test_pred_dataset, labels=['Pred']).get_figure().savefig('subset_test_dataset.png')


