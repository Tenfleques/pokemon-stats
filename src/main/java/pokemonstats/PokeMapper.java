package pokemonstats;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PokeMapper extends Mapper<Text, Text, Text, Pokemon> {
    @Override
    public void map(Text key, Text excelRow, Context context) throws IOException, InterruptedException{
            Pokemon pokemon = new Pokemon(excelRow.toString());
            context.write(pokemon.getType(), pokemon);
    }
}
