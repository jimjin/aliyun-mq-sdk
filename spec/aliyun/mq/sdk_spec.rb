require 'spec_helper'

describe Aliyun::Mq::Sdk do
  before(:each) do
    @access_key = Config::AK
    @secret_key = Config::SK
    @producer_1 = Config::PRODUCER_ID_1
    @consumer_1 = Config::CONSUMER_ID_1
    @topic_1 = Config::TOPIC_1
  end

  it 'has a version number' do
    expect(Aliyun::Mq::Sdk::VERSION).not_to be nil
  end

  it 'producer send order' do
    producer = Aliyun::Mq::Sdk::Producer.new(@access_key, @secret_key, @producer_1)
    r = producer.send('test order msg', topic: @topic_1, is_order: true)
    expect(r[:success]).to be true
    expect(r[:msgId]).not_to be nil
    expect(r[:sendStatus]).to be 'SEND_OK'
  end

  it 'consumer receive msg' do
    consumer = Aliyun::Mq::Sdk::Consumer.new(@access_key, @secret_key, @consumer_1)
    r = consumer.receive(topic: @topic_1)
    # [{“body”:”HelloMQ”, ”bornTime”:”1418973464204”, “msgHandle”:”X1BFTkRJTkdNU0dfXyVSRVRSWSUkbG9uZ2ppJENJRF9sb25namlfdGxvbmdqaQ==”, “msgId”:”0A021F7300002A9F000000000647076D”, ”reconsumeTimes”:1}]
    expect(r[:items].size) != 0
    expect(r[:success]).to be true
    expect(r[:items][0][:body]).not_to be nil
    expect(r[:items][0][:bornTime]).not_to be nil
    expect(r[:items][0][:msgHandle]).not_to be nil
    expect(r[:items][0][:msgId]).not_to be nil
    expect(r[:items][0][:reconsumeTimes]).to be 0
  end

  it 'consumer delete msg exists' do
    producer = Aliyun::Mq::Sdk::Producer.new(@access_key, @secret_key, @producer_1)
    producer.send('test order msg for delete', topic: @topic_1, is_order: true)

    consumer = Aliyun::Mq::Sdk::Consumer.new(@access_key, @secret_key, @consumer_1)
    r = consumer.receive(topic: @topic_1)

    r2 = consumer.delete(r[:items][0][:msgHandle], topic: @topic_1)
    expect(r2[:success]).to be true
  end

  it 'consumer delete msg not exists' do
    consumer = Aliyun::Mq::Sdk::Consumer.new(@access_key, @secret_key, @consumer_1)
    r = consumer.delete('not exists msg handle', topic: @topic_1)
    expect(r[:success]).to be false
    expect(r[:code]).to be "TOPIC_NOT_EXIST"
  end

  it 'msg order is true' do
    arr = []
    1.upto(50) do |i|
      producer = Aliyun::Mq::Sdk::Producer.new(@access_key, @secret_key, @producer_1)
      r = producer.send("test order is true msg #{i}", topic: @topic_1, is_order: true)
      arr << r[:msgId]
    end

    consumer = Aliyun::Mq::Sdk::Consumer.new(@access_key, @secret_key, @consumer_1)
    items = consumer.receive(topic: @topic_1)[:items]
    all_items = items
    while !items.empty?
      items = consumer.receive(topic: @topic_1)[:items]
      all_items += items
      sleep 2
    end
    p all_items.map {|item| item[:msgId]}
    p arr
    binding.pry
    expect(all_items.size).to be arr.size
    expect(all_items.map {|item| item[:msgId]}).to be arr
    all_items.each do |item|
      r55 = consumer.delete(item[:msgHandle], topic: @topic_1)
    end
  end
end
