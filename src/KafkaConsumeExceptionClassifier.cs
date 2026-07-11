using System;
using Confluent.Kafka;

namespace am.kon.packages.services.kafka
{
    internal static class KafkaConsumeExceptionClassifier
    {
        internal static bool IsAutoOffsetResetError(Exception exception)
        {
            return exception is ConsumeException consumeException
                && consumeException.Error.Code == ErrorCode.Local_AutoOffsetReset;
        }
    }
}
