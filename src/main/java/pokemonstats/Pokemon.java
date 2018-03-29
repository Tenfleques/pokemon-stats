package pokemonstats;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pokemon implements Writable {
    private Text name;
    private Text type;
    private DoubleWritable power;
    private DoubleWritable hp;
    private DoubleWritable attack;
    private DoubleWritable defence;
    private DoubleWritable specialAttack;
    private DoubleWritable specialDefence;
    private DoubleWritable speed;

    private static final int NAME = 1;
    private static final int TYPE = 2;
    private static final int POWER = 3;
    private static final int HP = 4;
    private static final int ATTACK = 5;
    private static final int SPECIAL_ATTACK = 7;
    private static final int DEFENCE = 6;
    private static final int SPECIAL_DEFENCE = 8;
    private static final int SPEED = 9;

    public Pokemon(){
        name = new Text();
        type = new Text();
        power = new DoubleWritable();
        hp = new DoubleWritable();
        attack = new DoubleWritable();
        defence = new DoubleWritable();
        specialAttack = new DoubleWritable();
        specialDefence = new DoubleWritable();
        speed = new DoubleWritable();
    }
    public Pokemon(Pokemon p){
        name = new Text(p.name);
        type = new Text(p.type);
        power = new DoubleWritable(p.power.get());
        hp = new DoubleWritable(p.hp.get());
        attack = new DoubleWritable(p.attack.get());
        defence = new DoubleWritable(p.defence.get());
        specialAttack = new DoubleWritable(p.specialAttack.get());
        specialDefence = new DoubleWritable(p.specialDefence.get());
        speed = new DoubleWritable(p.speed.get());
    }
    public Pokemon(String csvRow) {
        String[] row = csvRow.split(",");

        type = new Text(row[TYPE]);
        name = new Text(row[NAME]);
        try {
            power = new DoubleWritable(Double.parseDouble(row[POWER]));
        }catch (NumberFormatException e){
            power = new DoubleWritable(0);
        }
        try{
            hp = new DoubleWritable(Double.parseDouble(row[HP]));
        }catch (NumberFormatException e){
            hp = new DoubleWritable(0);
        }
        try{
            attack = new DoubleWritable(Double.parseDouble(row[ATTACK]));
        }catch (NumberFormatException e){
            attack = new DoubleWritable(0);
        }
        try{
            specialAttack = new DoubleWritable(Double.parseDouble(row[SPECIAL_ATTACK]));
        }catch (NumberFormatException e){
            specialAttack = new DoubleWritable(0);
        }
        try{
            defence = new DoubleWritable(Double.parseDouble(row[DEFENCE]));
        }catch (NumberFormatException e){
            defence = new DoubleWritable(0);
        }
        try{
            specialDefence = new DoubleWritable(Double.parseDouble(row[SPECIAL_DEFENCE]));
        }catch (NumberFormatException e){
            specialDefence = new DoubleWritable(0);
        }
        try{
            speed = new DoubleWritable(Double.parseDouble(row[SPEED]));
        }catch (NumberFormatException e){
            speed = new DoubleWritable(0);
        }
    }
    @Override
    public String toString(){
        return "0"
                +","+name
                +","+type
                +","+power.get()
                +","+hp.get()
                +","+attack.get()
                +","+defence.get()
                +","+specialAttack.get()
                +","+specialDefence.get()
                +","+speed.get();
    }
    public void write(DataOutput dataOutput) throws IOException {
       name.write(dataOutput);
       type.write(dataOutput);
       hp.write(dataOutput);
       attack.write(dataOutput);
       specialAttack.write(dataOutput);
       defence.write(dataOutput);
       specialDefence.write(dataOutput);
       speed.write(dataOutput);
    }
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
        type.readFields(dataInput);
        hp.readFields(dataInput);
        attack.readFields(dataInput);
        specialAttack.readFields(dataInput);
        defence.readFields(dataInput);
        specialDefence.readFields(dataInput);
        speed.readFields(dataInput);
    }

    public String getName() {
        return String.valueOf(name);
    }

    public Text getType() {
        return type;
    }

    public DoubleWritable getPower() {
        return power;
    }

    public DoubleWritable getHp() {
        return hp;
    }

    public DoubleWritable getAttack() {
        return attack;
    }

    public DoubleWritable getDefence() {
        return defence;
    }

    public DoubleWritable getSpecialAttack() {
        return specialAttack;
    }

    public DoubleWritable getSpecialDefence() {
        return specialDefence;
    }

    public DoubleWritable getSpeed() {
        return speed;
    }
    public  DoubleWritable getAllDefence(){
        double tmp = getDefence().get() + getSpecialDefence().get();
        DoubleWritable returnValue = new DoubleWritable();
        returnValue.set(tmp);
        return returnValue;
    }
    public DoubleWritable getAllAttack(){
        double tmp = getAttack().get() + getSpecialAttack().get();
        DoubleWritable returnValue = new DoubleWritable();
        returnValue.set(tmp);
        return returnValue;
    }
}
