using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Retained;
using System.Buffers;
using System.Text;

namespace MqttZero.Tests.Retained;

public sealed class RetainedTopicTrieTests
{
    [Fact]
    public void MatchesRetainedTopicsWithoutEnumeratingTheDictionary()
    {
        MqttZeroRetainedMessage first = Message("devices/a/state");
        MqttZeroRetainedMessage second = Message("devices/b/state");
        MqttZeroRetainedMessage other = Message("other/a/state");
        MqttZeroRetainedTopicTrie trie =
            MqttZeroRetainedTopicTrie.Empty
                .Set(first.Topic.Span, first)
                .Set(second.Topic.Span, second)
                .Set(other.Topic.Span, other);
        List<MqttZeroRetainedMessage> results = [];

        try
        {
            trie.Match("devices/+/state"u8, results);

            Assert.Equal(
                ["devices/a/state", "devices/b/state"],
                results.Select(message =>
                    Encoding.UTF8.GetString(message.Topic.Span)));
        }
        finally
        {
            first.Release();
            second.Release();
            other.Release();
        }
    }

    [Fact]
    public void MultiLevelWildcardIncludesTheParentTopic()
    {
        MqttZeroRetainedMessage parent = Message("devices");
        MqttZeroRetainedMessage child = Message("devices/a/state");
        MqttZeroRetainedMessage other = Message("other/a/state");
        MqttZeroRetainedTopicTrie trie =
            MqttZeroRetainedTopicTrie.Empty
                .Set(parent.Topic.Span, parent)
                .Set(child.Topic.Span, child)
                .Set(other.Topic.Span, other);
        List<MqttZeroRetainedMessage> results = [];

        try
        {
            trie.Match("devices/#"u8, results);

            Assert.Equal(
                ["devices", "devices/a/state"],
                results.Select(message =>
                    Encoding.UTF8.GetString(message.Topic.Span)));
        }
        finally
        {
            parent.Release();
            child.Release();
            other.Release();
        }
    }

    [Theory]
    [InlineData("foo", "foo", 1)]
    [InlineData("foo", "foo/", 0)]
    [InlineData("foo/+", "foo/", 1)]
    [InlineData("foo/", "foo/", 1)]
    [InlineData("+/state", "/state", 1)]
    [InlineData("#", "devices/a", 1)]
    public void MatchPreservesTrailingEmptyTopicLevels(
        string filter,
        string topic,
        int expectedCount)
    {
        MqttZeroRetainedMessage message = Message(topic);
        MqttZeroRetainedTopicTrie trie =
            MqttZeroRetainedTopicTrie.Empty
                .Set(message.Topic.Span, message);
        List<MqttZeroRetainedMessage> results = [];

        try
        {
            trie.Match(Encoding.UTF8.GetBytes(filter), results);

            Assert.Equal(expectedCount, results.Count);
        }
        finally
        {
            message.Release();
        }
    }

    [Fact]
    public void SetAndRemoveDoNotMutatePreviousTrie()
    {
        MqttZeroRetainedMessage first = Message("route/one");
        MqttZeroRetainedMessage second = Message("route/two");
        MqttZeroRetainedTopicTrie initial = MqttZeroRetainedTopicTrie.Empty
            .Set(first.Topic.Span, first);
        MqttZeroRetainedTopicTrie updated = initial
            .Set(second.Topic.Span, second);
        MqttZeroRetainedTopicTrie removed = updated
            .Remove(first.Topic.Span);
        List<MqttZeroRetainedMessage> initialResults = [];
        List<MqttZeroRetainedMessage> updatedResults = [];
        List<MqttZeroRetainedMessage> removedResults = [];

        try
        {
            initial.Match("route/#"u8, initialResults);
            updated.Match("route/#"u8, updatedResults);
            removed.Match("route/#"u8, removedResults);

            Assert.Single(initialResults);
            Assert.Equal(2, updatedResults.Count);
            Assert.Same(second, Assert.Single(removedResults));
        }
        finally
        {
            first.Release();
            second.Release();
        }
    }

    [Fact]
    public void MatchingDoesNotAllocateAfterWarmup()
    {
        MqttZeroRetainedMessage message = Message("devices/one/state");
        MqttZeroRetainedTopicTrie trie =
            MqttZeroRetainedTopicTrie.Empty
                .Set(message.Topic.Span, message);
        List<MqttZeroRetainedMessage> results = new(8);

        try
        {
            for (int index = 0; index < 10; index++)
            {
                results.Clear();
                trie.Match("devices/+/state"u8, results);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                results.Clear();
                trie.Match("devices/+/state"u8, results);
            }

            Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
        }
        finally
        {
            message.Release();
        }
    }

    private static MqttZeroRetainedMessage Message(string topic)
    {
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(topic)),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>([1]),
            MqttQualityOfServiceLevel.AtLeastOnce,
            true,
            null,
            null);
        try
        {
            return new MqttZeroRetainedMessage(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                null);
        }
        finally
        {
            lease.Release();
        }
    }
}
