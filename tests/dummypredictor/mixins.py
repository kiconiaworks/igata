from igata.handlers.aws.output.mixins import PostPredictHookMixInBase


class DummyMixin(PostPredictHookMixInBase):
    def mixin_method(self) -> bool:
        return True
