module Aliyun::Mq::Sdk
  class producer
    include HTTParty

    # 公有云生产环境：http://onsaddr-internal.aliyun.com:8080/rocketmq/nsaddr4client-internal
    # 公有云公测环境：http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet
    # 杭州金融云环境：http://jbponsaddr-internal.aliyun.com:8080/rocketmq/nsaddr4client-internal
    # 杭州深圳云环境：http://mq4finance-sz.addr.aliyun.com:8080/rocketmq/nsaddr4client-internal
    DEFAULT_BASE_URI = 'http://onsaddr-internal.aliyun.com:8080/rocketmq/nsaddr4client-internal'

    attr_accessor :access_key, :secret_key, :producer_id, :region_url, :default_topic, :topic

    def initialize(access_key, secret_key, producer_id, opts={})
      @access_key = access_key
      @secret_key = secret_key
      @producer_id = producer_id

      @region_url = opts[:region_url] || DEFAULT_BASE_URI
      @default_topic = opts[:default_topic]
    end

    def headers(msg, time)
      sign = Auth.post_sign(secret_key, topic, producer_id, msg, time)
      {"Signature" => sign, "AccessKey" => access_key, "ProducerID" => producer_id}
    end

    def send(msg, opts={})
      @time = Time.now.to_i.to_s
      @topic = opts[:topic] || default_topic
      tag = opts[:tag]
      key = opts[:key]
      is_order = opts[:is_order]
      sharding_key = opts[:sharding_key]

      query = {"topic" => topic, "time" => @time}

      query["tag"] = tag if tag
      query["key"] = key if key

      if is_order && sharding_key != null
        query = query.merge("isOrder" => is_order, "shardingKey" => sharding_key)
      end

      self.class.post(region_url, headers: headers(msg, @time), query: query)
    end
  end
end