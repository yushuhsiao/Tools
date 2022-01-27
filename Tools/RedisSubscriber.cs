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

        internal RedisSubscriber(IServiceProvider service, ISubscriber subscriber, string configuration, double timeout)
            : base(configuration, timeout)
        {
            this.subscriber = subscriber;
            this._logger = service?.GetService<ILogger<RedisConnection>>();
        }

        private RedisSubscriber(RedisSubscriber src)
            : base(src.configuration, src.timeout)
        {
            this.subscriber = ConnectionMultiplexer.Connect(this.configuration).GetSubscriber();
            this._logger = src._logger;
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
            using (subscriber?.Multiplexer)
                subscriber = null;
        }

        private class Handler
        {
            public string Channel { get; set; }
            public MessageHandler OnMessage { get; set; }
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
            }
            subscriber.Subscribe(channel, OnMessage);
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
            }
            subscriber.Unsubscribe(channel);
        }

        void OnMessage(RedisChannel channel, RedisValue message)
        {
            _logger.LogInformation($"redis subscribe : {channel}, {message}");
            var handlers = Interlocked.CompareExchange(ref handlers2, null, null);
            for (int i = 0; i < handlers.Length; i++)
            {
                try
                {
                    handlers[i].OnMessage(channel, message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
        }
    }
}