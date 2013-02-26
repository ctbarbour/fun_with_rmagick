require 'RMagick'
require 'celluloid'
require 'benchmark'
require 'socket'
require 'msgpack'

class ImageWriter
  def initialize(image_list)
    @image_list = image_list
  end

  private

  attr_reader :image_list
end

class MultiPageImageWriter < ImageWriter
  def write(sink)
    image_list.write(sink)
  end
end

class SinglePageImageWriter < ImageWriter
  def write(sink)
    sink_name = Pathname.new(sink)
    base = File.join(sink_name.dirname, sink_name.basename.to_s.gsub(sink_name.extname, ''))

    image_list.each_with_index do |image, number|
      image.write("#{base}_#{number + 1}#{sink_name.extname}")
    end
  end
end

class Endorser

  def initialize(endorsement, &block)
    @endorsement          = endorsement
    @pointsize            = 10
    @font_family          = 'helvetica'
    @font_weight          = Magick::BoldWeight
    @image_writer         = MultiPageImageWriter.method(:new)
    @endorsement_position = Magick::SouthWestGravity
    @bates_position       = Magick::SouthEastGravity

    block.arity > 0 ? block.call(self) : self.instance_eval(&block) if block_given?
  end

  def info(message)
    puts "#{self.class} [#{Thread.current.object_id}] #{message}"
  end

  def endorse(source, sink, starting_bates)
    bates_number = starting_bates
    x_margin_padding = 10
    y_margin_padding = 10

    pages = Magick::Image.read(source).inject(Magick::ImageList.new) do |pages, page|
      #info "Endorsing #{source} (#{bates_number})"
      endorsement_draw(page).annotate(page, 0, 0, x_margin_padding, y_margin_padding, @endorsement)
      bates_draw(page).annotate(page, 0, 0, x_margin_padding, y_margin_padding, bates_number.to_s)

      #info "Done #{source} (#{bates_number})"
      bates_number = bates_number.next
      pages << page
    end

    @image_writer.call(pages).write(sink)
    pages.size
  end

  def image_writer(value)
    @image_writer = value.method(:new)
    self
  end

  def font_family(value)
    @font_family = value
    self
  end

  def pointsize(value)
    @pointsize = value
    self
  end

  def font_weight(value)
    @font_weight = value
    self
  end

  def endorsement_position(value)
    @endorsement_position = value
    self
  end

  def bates_position(value)
    @bates_position = value
    self
  end

  private

  def base_draw(image)
    base_draw              = Magick::Draw.new
    base_draw.density      = image.density
    base_draw.font_family  = @font_family
    base_draw.pointsize    = @pointsize
    base_draw.font_weight  = @font_weight
    base_draw
  end

  def endorsement_draw(image)
    e_draw = base_draw(image)
    e_draw.gravity = @endorsement_position
    e_draw
  end

  def bates_draw(image)
    bates_draw = base_draw(image)
    bates_draw.gravity= @bates_position
    bates_draw
  end
end

class BatesNumber
  include Comparable

  class InvalidBatesNumber < StandardError
  end

  def initialize(prefix, number = 1, padding = 8)
    raise InvalidBatesNumber, "Number must be greater than 0. (#{number})" unless number > 0
    @prefix = prefix
    @number = number
    @padding = padding
  end
  attr_reader :prefix, :number, :padding

  def to_s
    "#{prefix}%0#{padding}d" % number
  end

  def next
    BatesNumber.new(prefix, number + 1, padding)
  end

  def previous
    BatesNumber.new(prefix, number - 1, padding)
  end

  def <=>(other)
    to_s <=> other.to_s
  end

  def inspect
    to_s
  end
end

class EndorsingActor
  include Celluloid

  def initialize(endorsement)
    #@endorser = Endorser.new(endorsement)
    @endorsement = endorsement
  end

  def endorse(source, sink, starting_bates)
    child_socket, parent_socket = Socket.pair(:UNIX, :DGRAM, 0)
    maxlen = 10000

    pid = fork do
      parent_socket.close
      source, out = MessagePack.unpack(child_socket.recv(maxlen))

      start_bates = BatesNumber.new("TEST_")

      endorser = Endorser.new("CONFIDENTIAL")
      result = endorser.endorse(source, out, starting_bates)

      child_socket.send(MessagePack.pack(result), 0)
    end

    child_socket.close

    out = File.join(sink, File.basename(source))
    message = MessagePack.pack([ source, out ])
    parent_socket.send(message, 0)

    response = parent_socket.recv(maxlen)
    MessagePack.unpack(response)
  end
end

class EndorsingPool < Celluloid::SupervisionGroup
  pool EndorsingActor, :as => :endorsing_pool, :size => 2, :args => [ "CONFIDENTIAL" ]
end

class FileExplorer
  def initialize(source)
    @source = File.join(source, '**', '*.tif')
  end

  def explore(&block)
    Dir.glob(@source).map{ |d| block.call(d) }
  end
end

if __FILE__ == $0

  EndorsingPool.run!
  #workers = Celluloid::Actor[:endorsing_pool]

  workers = EndorsingActor.new("CONFIDENTIAL")

  bm_results = Benchmark.bmbm do |bm|

    bm.report(:async) do
      results = FileExplorer.new(ARGV[0]).explore do |fd|
        workers.future(:endorse, fd, ARGV[1], BatesNumber.new("TEST_"))
      end

      #puts "Endorsed #{results.map(&:value).reduce(0, :+)} pages"
    end

    bm.report(:sync) do
      endorser = Endorser.new("CONFIDENTIAL")

      FileExplorer.new(ARGV[0]).explore do |fd|
        sink = File.join(ARGV[1], File.basename(fd))
        endorser.endorse(fd, sink, BatesNumber.new("TEST_"))
      end
    end
  end
end
