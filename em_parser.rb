#https://cars.av.by/filter
require 'thread/pool'
require 'curb'
require 'nokogiri'
require 'sqlite3'
require 'pry'
require 'time'
require 'open-uri'
require 'eventmachine'
require 'em-http-request'
require 'byebug'

t = Time.now
site = ('https://divanby.com')
puts "Site #{site}"

urls=[]
start_page="https://divanby.com/catalog/?page="
(1..77).each do |p|
  url=start_page+p.to_s
  urls<<url
end
concurrency = 32
items = []
EM.run do
  EM::Iterator.new(urls, concurrency).each(
      proc { |url, iter|
        http = EventMachine::HttpRequest.new(url, ssl: {verify_peer: false}, :connect_timeout => 10).get
        http.callback do |response|
          document = Nokogiri::HTML(response.response)
          document.xpath('//*/div[@class="view-content catalog__cards"]/*/a/@href').each do |item|
            items<<item
          end
          iter.next
        end
        http.errback do
          p "Failed: #{url}"
          iter.next
        end
      },
      proc {
        p 'Pages parsed'
        EM.stop
      })
end
product_links = []
items.map do |item|
  link = site + item
  product_links << link
end
puts "#{product_links.length} links"
products=[]
EM.run do
  EM::Iterator.new(product_links, concurrency).each(
      proc { |url, iter|
        http = EventMachine::HttpRequest.new(url, ssl: {verify_peer: false}, :connect_timeout => 20).get
        http.callback do |response|
          document = Nokogiri::HTML(response.response)
          product = {
              :name => document.xpath('//section[@class="kartochka"]//div[@class="name"]/h3/text()').to_s,
              :price => document.xpath('//section[@class="kartochka"]//div[@class="new-price"]/text()').to_s.delete!('руб.').strip,
              :image => document.xpath('//section[@class="kartochka"]//li[1]/a[@class="fancybox_a"]/img/@src'),
              :link => url
          }
          products<<product
          iter.next
        end
        http.errback do
          p "Failed: #{url}"
          iter.next
        end
      },
      proc {
        puts "#{products.length} products parsed"
        EM.stop
      })
end
#puts products.length
# products.each do |product|
#   product.each do |key, value|
#   puts "#{key}:#{value}"
#   end
# end
puts (t - Time.now).abs.round(2)

t=Time.now
puts "Writing in db"
db = SQLite3::Database.open("divanby.db")
db.transaction do |db|
db.execute("DELETE FROM products")
products.each do |product|
  db.execute("INSERT INTO products (name, price, image, link)
             VALUES (?, ?, ?, ?)",
             product[:name], product[:price], product[:image].to_s, product[:link], )
end
end
db.close
puts "DB done!"

t = (t - Time.now).abs.round(2)
puts t