package pokemonstats;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PokeReducer extends Reducer<Text, Pokemon, Text, Text>  {
    @Override
    public void reduce(Text key, Iterable<Pokemon> listOfPokemon, Context context)
            throws IOException, InterruptedException {
        Pokemon tank = new Pokemon();
        Pokemon feeble = new Pokemon();
        Pokemon defender = new Pokemon();
        Pokemon slowpoke = new Pokemon();
        boolean initialized = false;
        for (Pokemon pokemon : listOfPokemon) {
            if(!initialized){//adopt values of the first element for all resultant pokemon
                tank = new Pokemon(pokemon);
                feeble = new Pokemon(pokemon);
                defender = new Pokemon(pokemon);
                slowpoke = new Pokemon(pokemon);
                initialized = true;
                continue;
            }
            if(pokemon.getHp().compareTo(tank.getHp()) > 0)
                tank = new Pokemon(pokemon);

            if(pokemon.getAllAttack().compareTo(feeble.getAllAttack()) < 0)
                feeble = new Pokemon(pokemon);

            if(pokemon.getAllDefence().compareTo(defender.getAllDefence()) > 0)
                defender = new Pokemon(pokemon);

            if(pokemon.getSpeed().compareTo(slowpoke.getSpeed()) < 0)
                slowpoke = new Pokemon(pokemon);
        }
        Text val = new Text(
                        tank.getName()
                        + "," + feeble.getName()
                        + "," + defender.getName()
                        + "," + slowpoke.getName());
        context.write(key, val);
    }
}