import logging
import time

import backoff
# from langchain_community.chat_models import ChatTongyi as _ChatModel
from langchain_openai import ChatOpenAI as _ChatModel
from langchain_core.messages import AIMessage

import CONFIG
import utils
from CONFIG import API_KEY as _api_key
from CONFIG import LLM_TYPE as _model



logger = logging.getLogger(__name__)


class _SynchronizedChatModel(_ChatModel):
    def __init__(self, **kwargs):
        super(_SynchronizedChatModel, self).__init__(**kwargs)

    # @utils.semaphore(CONFIG.LLM_MAX_THREADS)
    @backoff.on_exception(backoff.expo, Exception, max_tries=CONFIG.MAX_TRY,raise_on_giveup=True)
    # @utils.semaphore(CONFIG.LLM_RPM)
    @utils.rate_limited(qpm=CONFIG.LLM_RPM)
    async def ainvoke(self, *args, **kwargs):
        start_time = time.time()

        try:
            result = await super().ainvoke(*args, **kwargs)

            end_time = time.time()
            duration = end_time - start_time
            # 你可以改成写数据库、写文件、打 metrics 等
            if duration > CONFIG.TIME_WARN:
                logger.info(
                    f"[LLM CALL] model={self.model_name}, "
                    f"duration={duration:.3f}s"
                )
            return result
        except ValueError as e:#捕获内容敏感错误，图片解析错误，并直接返回空
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"[LLM CALL Error] {e} , duration={duration:.3f}s")
            return AIMessage("")


_llm = _SynchronizedChatModel(
    api_key=_api_key,
    model=_model,
base_url=CONFIG.API_BASE
)



def get_llm():
    global _llm
    return _llm

#
# class _EmbeddingClient:
#     def __init__(self, model: str = CONFIG.EMBEDDING_MODEL):
#         """
#         初始化 OpenAI Embedding 客户端。
#         model: 使用的模型，默认使用 'text-embedding-v2'。
#         """
#         self.client = AsyncOpenAI(api_key=CONFIG.API_KEY, base_url=CONFIG.API_BASE)
#         self.model = model
#
#
#
#     # 修改后的 aget_text_embedding 支持批量请求
#     @utils.rate_limited(qpm=100)
#     async def aget_batch_text_embedding(self, texts: List[str]) -> ndarray[Any]:
#         """
#         批量获取文本的嵌入向量。保证每个批次最多 10 个文本。
#         texts: 一组文本（List[str]）。
#         返回文本嵌入向量的列表（List[List[float]]）。
#         """
#         if len(texts) == 0:
#             return np.array([])
#
#         # 限制每个文本的最大长度
#         texts = [text[:CONFIG.KNOWLEDGE_MAX_LENGTH] for text in texts]
#
#         embeddings = []
#         batch_size = 10  # 最大批次大小为 10
#
#         # 分批处理文本，确保每批最多 10 个句子
#         for i in range(0, len(texts), batch_size):
#             batch = texts[i:i + batch_size]  # 获取当前批次
#             try:
#                 # 调用私有方法来处理每个批次，并启用重试机制
#                 batch_embeddings = await self._try_batch_request(batch)
#                 embeddings.extend(batch_embeddings)  # 将批次结果添加到最终列表中
#             except Exception as e:
#                 # 如果批次请求失败，则记录异常
#                 logger.error(f"[Error] Failed to process batch starting with sentence: {batch[0]}. Error: {e}")
#
#         if any(e is None for e in embeddings):
#             raise RuntimeError("Failed to compute some embeddings")
#
#         embeddings_array = np.vstack([np.asarray(emb, dtype=np.float32) for emb in embeddings])
#         return embeddings_array
#
#     @backoff.on_exception(backoff.expo, Exception, max_tries=CONFIG.MAX_TRY,raise_on_giveup=True)
#     async def _try_batch_request(self, batch: List[str]) -> List[List[float]]:
#         """
#         尝试请求一个批次的嵌入，并启用 backoff 重试机制。
#         batch: 要请求的文本批次（List[str]）。
#         返回文本嵌入向量的列表（List[List[float]]）。
#         """
#         start_time = time.time()
#         try:
#             # 批量请求嵌入
#             response = await self.client.embeddings.create(
#                 model=self.model,
#                 input=batch,  # 直接传入当前批次的文本
#                 dimensions=CONFIG.VECTOR_DIM,  # 指定向量维度
#                 encoding_format="float"
#             )
#             embeddings = [res.embedding for res in response.data]  # 提取每个文本的嵌入向量
#             end_time = time.time()  # 记录结束时间
#
#             # 打印响应的 id 和调用时间
#             response_id = response.model_extra["id"]
#             duration = end_time - start_time
#             if duration > CONFIG.TIME_WARN:
#                 logger.info(f"[Text Embedding] Response ID: {response_id}, API call took {duration:.3f} seconds.")
#
#             if any(e is None for e in embeddings):
#                 end_time = time.time()
#                 duration = end_time - start_time
#                 raise RuntimeError(f"Failed to compute some embeddings after {duration:.3f}")
#
#             return embeddings
#
#         except Exception as e:
#             end_time = time.time()
#             duration = end_time - start_time
#             raise Exception(f"[Error] Text embedding failed after {duration:.3f}: {e}")
#
#
# _embed_client = _EmbeddingClient()
#
#
# def get_embed_client():
#     global _embed_client
#     return _embed_client
#
#
# def MultimodalityMessage(query,*args)->HumanMessage:
#     """
#     :param query: 提示词
#     :param kwargs: 多个图像url
#     :return:
#     """
#     content = [
#         *[{"type": "image","image":url} for url in args],
#         {
#             "type": "text",
#             "text": query
#         }
#     ]
#     return HumanMessage(content)
