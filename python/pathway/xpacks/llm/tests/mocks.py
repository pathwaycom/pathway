import pathway as pw
from pathway.xpacks.llm import llms


class IdentityMockChat(llms.BaseChat):
    def _accepts_call_arg(self, arg_name: str) -> bool:
        return False

    async def __wrapped__(self, messages: list[dict] | pw.Json, model: str) -> str:
        return model + "," + messages[0]["content"].as_str()
