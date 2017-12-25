package com.arv.bigdata;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Hello world!
 *
 */
public class NameProducer {

    private static String[] left = { "admiring", "adoring", "affectionate", "agitated", "amazing", "angry", "awesome",
            "blissful", "boring", "brave", "clever", "cocky", "compassionate", "competent", "condescending",
            "confident", "cranky", "dazzling", "determined", "distracted", "dreamy", "eager", "ecstatic", "elastic",
            "elated", "elegant", "eloquent", "epic", "fervent", "festive", "flamboyant", "focused", "friendly",
            "frosty", "gallant", "gifted", "goofy", "gracious", "happy", "hardcore" };

    private static String[] right = { // Muhammad ibn Jabir al-?arrani al-Battani was a founding father of astronomy.
            // https://en.wikipedia.org/wiki/Mu%E1%B8%A5ammad_ibn_J%C4%81bir_al-%E1%B8%A4arr%C4%81n%C4%AB_al-Batt%C4%81n%C4%AB
            "albattani",

            // Frances E. Allen, became the first female IBM Fellow in 1989. In 2006, she became the first female
            // recipient of the ACM's Turing Award. https://en.wikipedia.org/wiki/Frances_E._Allen
            "allen",

            // June Almeida - Scottish virologist who took the first pictures of the rubella virus -
            // https://en.wikipedia.org/wiki/June_Almeida
            "almeida",

            // Maria Gaetana Agnesi - Italian mathematician, philosopher, theologian and humanitarian. She was the first
            // woman to write a mathematics handbook and the first woman appointed as a Mathematics Professor at a
            // University. https://en.wikipedia.org/wiki/Maria_Gaetana_Agnesi
            "agnesi",

            // Archimedes was a physicist, engineer and mathematician who invented too many things to list them here.
            // https://en.wikipedia.org/wiki/Archimedes
            "archimedes",

            // Maria Ardinghelli - Italian translator, mathematician and physicist -
            // https://en.wikipedia.org/wiki/Maria_Ardinghelli
            "ardinghelli",

            // Aryabhata - Ancient Indian mathematician-astronomer during 476-550 CE
            // https://en.wikipedia.org/wiki/Aryabhata
            "aryabhata",

            // Wanda Austin - Wanda Austin is the President and CEO of The Aerospace Corporation, a leading architect
            // for the US security space programs. https://en.wikipedia.org/wiki/Wanda_Austin
            "austin",

            // Charles Babbage invented the concept of a programmable computer.
            // https://en.wikipedia.org/wiki/Charles_Babbage.
            "babbage",

            // Stefan Banach - Polish mathematician, was one of the founders of modern functional analysis.
            // https://en.wikipedia.org/wiki/Stefan_Banach
            "banach",

            // John Bardeen co-invented the transistor - https://en.wikipedia.org/wiki/John_Bardeen
            "bardeen",

            // Jean Bartik, born Betty Jean Jennings, was one of the original programmers for the ENIAC computer.
            // https://en.wikipedia.org/wiki/Jean_Bartik
            "bartik",

            // Laura Bassi, the world's first female professor https://en.wikipedia.org/wiki/Laura_Bassi
            "bassi",

            // Hugh Beaver, British engineer, founder of the Guinness Book of World Records
            // https://en.wikipedia.org/wiki/Hugh_Beaver
            "beaver",

            // Alexander Graham Bell - an eminent Scottish-born scientist, inventor, engineer and innovator who is
            // credited with inventing the first practical telephone -
            // https://en.wikipedia.org/wiki/Alexander_Graham_Bell
            "bell",

            // Karl Friedrich Benz - a German automobile engineer. Inventor of the first practical motorcar.
            // https://en.wikipedia.org/wiki/Karl_Benz
            "benz",

            // Homi J Bhabha - was an Indian nuclear physicist, founding director, and professor of physics at the Tata
            // Institute of Fundamental Research. Colloquially known as "father of Indian nuclear programme"-
            // https://en.wikipedia.org/wiki/Homi_J._Bhabha
            "bhabha",

            // Bhaskara II - Ancient Indian mathematician-astronomer whose work on calculus predates Newton and Leibniz
            // by over half a millennium - https://en.wikipedia.org/wiki/Bh%C4%81skara_II#Calculus
            "bhaskara",

            // Elizabeth Blackwell - American doctor and first American woman to receive a medical degree -
            // https://en.wikipedia.org/wiki/Elizabeth_Blackwell
            "blackwell",

            // Niels Bohr is the father of quantum theory. https://en.wikipedia.org/wiki/Niels_Bohr.
            "bohr",

            // Kathleen Booth, she's credited with writing the first assembly language.
            // https://en.wikipedia.org/wiki/Kathleen_Booth
            "booth",

            // Anita Borg - Anita Borg was the founding director of the Institute for Women and Technology (IWT).
            // https://en.wikipedia.org/wiki/Anita_Borg
            "borg",

            // Satyendra Nath Bose - He provided the foundation for BoseEinstein statistics and the theory of the
            // BoseEinstein condensate. - https://en.wikipedia.org/wiki/Satyendra_Nath_Bose
            "bose",

            // Evelyn Boyd Granville - She was one of the first African-American woman to receive a Ph.D. in
            // mathematics; she earned it in 1949 from Yale University.
            // https://en.wikipedia.org/wiki/Evelyn_Boyd_Granville
            "boyd",

            // Brahmagupta - Ancient Indian mathematician during 598-670 CE who gave rules to compute with zero -
            // https://en.wikipedia.org/wiki/Brahmagupta#Zero
            "brahmagupta",

            // Walter Houser Brattain co-invented the transistor - https://en.wikipedia.org/wiki/Walter_Houser_Brattain
            "brattain",

            // Emmett Brown invented time travel. https://en.wikipedia.org/wiki/Emmett_Brown (thanks Brian Goff)
            "brown",

            // Rachel Carson - American marine biologist and conservationist, her book Silent Spring and other writings
            // are credited with advancing the global environmental movement.
            // https://en.wikipedia.org/wiki/Rachel_Carson
            "carson",

            // Subrahmanyan Chandrasekhar - Astrophysicist known for his mathematical theory on different stages and
            // evolution in structures of the stars. He has won nobel prize for physics -
            // https://en.wikipedia.org/wiki/Subrahmanyan_Chandrasekhar
            "chandrasekhar",

            // Asima Chatterjee was an indian organic chemist noted for her research on vinca alkaloids, development of
            // drugs for treatment of epilepsy and malaria - https://en.wikipedia.org/wiki/Asima_Chatterjee
            "chatterjee",

            // Claude Shannon - The father of information theory and founder of digital circuit design theory.
            // (https://en.wikipedia.org/wiki/Claude_Shannon)
            "shannon",

            // Joan Clarke - Bletchley Park code breaker during the Second World War who pioneered techniques that
            // remained top secret for decades. Also an accomplished numismatist
            // https://en.wikipedia.org/wiki/Joan_Clarke
            "clarke",

            // Jane Colden - American botanist widely considered the first female American botanist -
            // https://en.wikipedia.org/wiki/Jane_Colden
            "colden",

            // Gerty Theresa Cori - American biochemist who became the third womanand first American womanto win a
            // Nobel Prize in science, and the first woman to be awarded the Nobel Prize in Physiology or Medicine. Cori
            // was born in Prague. https://en.wikipedia.org/wiki/Gerty_Cori
            "cori",

            // Seymour Roger Cray was an American electrical engineer and supercomputer architect who designed a series
            // of computers that were the fastest in the world for decades. https://en.wikipedia.org/wiki/Seymour_Cray
            "cray",

            // This entry reflects a husband and wife team who worked together:
            // Joan Curran was a Welsh scientist who developed radar and invented chaff, a radar countermeasure.
            // https://en.wikipedia.org/wiki/Joan_Curran
            // Samuel Curran was an Irish physicist who worked alongside his wife during WWII and invented the proximity
            // fuse. https://en.wikipedia.org/wiki/Samuel_Curran
            "curran",

            // Marie Curie discovered radioactivity. https://en.wikipedia.org/wiki/Marie_Curie.
            "curie",

            // Charles Darwin established the principles of natural evolution.
            // https://en.wikipedia.org/wiki/Charles_Darwin.
            "darwin",

            // Leonardo Da Vinci invented too many things to list here. https://en.wikipedia.org/wiki/Leonardo_da_Vinci.
            "davinci",

            // Edsger Wybe Dijkstra was a Dutch computer scientist and mathematical scientist.
            // https://en.wikipedia.org/wiki/Edsger_W._Dijkstra.
            "dijkstra",

            // Donna Dubinsky - played an integral role in the development of personal digital assistants (PDAs) serving
            // as CEO of Palm, Inc. and co-founding Handspring. https://en.wikipedia.org/wiki/Donna_Dubinsky
            "dubinsky" };

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Random random = new Random();

            while (true) {
                producer.send(new ProducerRecord<String, String>("test", left[random.nextInt(left.length)] + random.nextInt(),
                        right[random.nextInt(right.length)]));
                Thread.sleep(100);
            }
        }
    }
}
