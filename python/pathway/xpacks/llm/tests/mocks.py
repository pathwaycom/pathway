import pathway as pw
from pathway.xpacks.llm import llms


class IdentityMockChat(llms.BaseChat):
    def _accepts_call_arg(self, arg_name: str) -> bool:
        return False

    async def __wrapped__(self, messages: list[dict] | pw.Json, model: str) -> str:
        return model + "," + messages[0]["content"].as_str()


class FakeChatModel(llms.BaseChat):
    """Returns `"Text"` literal."""

    async def __wrapped__(self, *args, **kwargs) -> str:
        return "Text"

    def _accepts_call_arg(self, arg_name: str) -> bool:
        return True


@pw.udf
def fake_embeddings_model(x: str) -> list[float]:
    return [1.0, 1.0, 0.0]
