using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    public sealed class RedisSubscriber : RedisConnectionExtensions.RedisConnectionBase, IDisposable
    {
        public delegate void MessageHandler(string channel, string message);
        private ISubscriber subscriber;
        internal bool IsAlive => subscriber != null;
        private List<Handler> handlers1 = new List<Handler>();
        private Handler[] handlers2 = new Handler[0];
        private Action<RedisSubscriber> dispose;

        internal RedisSubscriber(IServiceProvider service, Action<RedisSubscriber> dispose, ISubscriber subscriber, string configuration, double timeout)
            : base(service?.GetService<ILogger<RedisConnection>>(), configuration, timeout)
        {
            this.subscriber = subscriber;
            this.dispose = dispose;
        }

        private RedisSubscriber(RedisSubscriber src)
            : base(src.logger, src.configuration, src.Timeout)
        {
            this.subscriber = ConnectionMultiplexer.Connect(this.configuration).GetSubscriber();
            this.dispose = src.dispose;
            lock (this.handlers1)
            {
                lock (src.handlers1)
                    this.handlers1.AddRange(src.handlers1);
                Interlocked.Exchange(ref this.handlers2, this.handlers1.ToArray());
            }
            foreach (var h in this.handlers2)
            {
                try { subscriber.Subscribe(h.Channel, this.OnMessage); }
                catch { }
            }
        }

        public RedisSubscriber Clone() => new RedisSubscriber(this);

        void IDisposable.Dispose()
        {
            lock (this.handlers1)
                using (subscriber?.Multiplexer)
                    subscriber = null;
            dispose(this);
        }

        private class Handler
        {
            public string Channel { get; set; }
            public MessageHandler OnMessage { get; set; }
        }

        public void Reset()
        {
            var subscriber = ConnectionMultiplexer.Connect(this.configuration).GetSubscriber();
            lock (this.handlers1)
            {
                using (this.subscriber?.Multiplexer)
                {
                    this.subscriber = subscriber;
                    foreach (var h in this.handlers2)
                    {
                        try { subscriber.Subscribe(h.Channel, this.OnMessage); }
                        catch { }
                    }
                }
            }
            base.timer.Reset();
        }

        public void Subscribe(string channel, MessageHandler handler)
        {
            lock (handlers1)
            {
                int cnt = 0;
                for (int i = 0; i < handlers1.Count; i++)
                {
                    var h = handlers1[i];
                    if (h.Channel == channel && object.ReferenceEquals(h.OnMessage, handler))
                        cnt++;
                }
                if (cnt == 0)
                {
                    handlers1.Add(new Handler() { Channel = channel, OnMessage = handler });
                    Interlocked.Exchange(ref handlers2, handlers1.ToArray());
                }
                subscriber.Subscribe(channel, OnMessage);
            }
        }

        public void UnSubscribe(string channel)
        {
            lock (handlers1)
            {
                int cnt = handlers1.Count;
                for (int i = cnt - 1; i >= 0; i--)
                {
                    var h = handlers1[i];
                    if (h.Channel == channel)
                        handlers1.RemoveAt(i);
                }
                if (cnt != handlers1.Count)
                    Interlocked.Exchange(ref handlers2, handlers1.ToArray());
                subscriber.Unsubscribe(channel);
            }
        }

        void OnMessage(RedisChannel channel, RedisValue message)
        {
            logger.LogInformation($"redis subscribe : {channel}, {message}");
            var handlers = Interlocked.CompareExchange(ref handlers2, null, null);
            for (int i = 0; i < handlers.Length; i++)
            {
                try
                {
                    handlers[i].OnMessage(channel, message);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                }
            }
        }
    }
}