# @dataclass
# class UppercaseOrError(Sink[str], ActorBuilder):
#     uppercased: Source[str]
#     error: Source[str]
#
#     def __init__(self):
#         super().__init__(name=self.__class__.__name__)
#
#     def process(self, ctx: ContextId, payload: str) -> None:
#         if len(payload) % 2 == 0:
#             self.error.publish(f'The input string has an even number of characters: {len(payload)}')
#         else:
#             self.uppercased.publish(payload.upper())
