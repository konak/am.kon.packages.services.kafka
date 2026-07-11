using Confluent.Kafka;

namespace am.kon.packages.services.kafka.tests;

public sealed class KafkaConsumeExceptionClassifierTests
{
    [Fact]
    public void IsAutoOffsetResetError_WhenConsumeErrorIsLocalAutoOffsetReset_ReturnsTrue()
    {
        var exception = CreateConsumeException(ErrorCode.Local_AutoOffsetReset);

        var result = KafkaConsumeExceptionClassifier.IsAutoOffsetResetError(exception);

        Assert.True(result);
    }

    [Theory]
    [InlineData(ErrorCode.Local_Application)]
    [InlineData(ErrorCode.OffsetOutOfRange)]
    public void IsAutoOffsetResetError_WhenConsumeErrorHasDifferentCode_ReturnsFalse(ErrorCode errorCode)
    {
        var exception = CreateConsumeException(errorCode);

        var result = KafkaConsumeExceptionClassifier.IsAutoOffsetResetError(exception);

        Assert.False(result);
    }

    [Fact]
    public void IsAutoOffsetResetError_WhenExceptionIsNotConsumeException_ReturnsFalse()
    {
        var result = KafkaConsumeExceptionClassifier.IsAutoOffsetResetError(new InvalidOperationException());

        Assert.False(result);
    }

    private static ConsumeException CreateConsumeException(ErrorCode errorCode)
    {
        return new ConsumeException(null, new Error(errorCode));
    }
}
