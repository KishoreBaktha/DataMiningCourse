package se.kth.jabeja;

import org.apache.log4j.Logger;
import se.kth.jabeja.config.Config;
import se.kth.jabeja.config.NodeSelectionPolicy;
import se.kth.jabeja.io.FileIO;
import se.kth.jabeja.rand.RandNoGenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static java.lang.Math.exp;
import static java.lang.Math.pow;

public class Check {
    final static Logger logger = Logger.getLogger(Jabeja.class);
    private final Config config;
    private final HashMap<Integer/*id*/, Node/*neighbors*/> entireGraph;
    private final List<Integer> nodeIds;
    private int numberOfSwaps;
    private int round;
    private float T;
    private boolean resultFileCreated = false;

    private Variation var = Variation.LINEAR;
    private boolean reset = false;
    private boolean smartRedistribute = true;
    //-------------------------------------------------------------------
    public Check(HashMap<Integer, Node> graph, Config config) {
        // Additional settings
        config.setNodeSelectionPolicy(NodeSelectionPolicy.RANDOM);
        switch (var) {
            case LINEAR:
                break;
            case EXPONENTIAL:
                config.setTemperature(1.0f);
                config.setDelta(0.99f);
                break;
        }

        this.entireGraph = graph;
        this.nodeIds = new ArrayList(entireGraph.keySet());
        this.round = 0;
        this.numberOfSwaps = 0;
        this.config = config;
        this.T = config.getTemperature();

        if (smartRedistribute) {
          //  Reinit.redistributeColors(entireGraph, config);
        }
    }

    //-------------------------------------------------------------------
    public void startJabeja() throws IOException {
        for (round = 0; round < config.getRounds(); round++) {
            for (int id : entireGraph.keySet()) {
                sampleAndSwap(id);
            }

            //one cycle for all nodes have completed.
            //reduce the temperature
            if (var == Variation.LINEAR)
                saCoolDownLinear();
            else if (var == Variation.EXPONENTIAL)
                saCoolDownExponential();
            report();
        }
    }
    /**
     * Simulated analealing cooling function
     */
    private void saCoolDownLinear() {
        if (T > 1) {
            T -= config.getDelta();
        } else {
            if (reset)
                T = config.getTemperature();
            else
                T = 1;
        }
    }

    private void saCoolDownExponential() {
        T *= config.getDelta();
        if (T < 0.001 && reset)
            T = config.getTemperature();
    }

    /**
     * Sample and swap algorith at node p
     *
     * @param nodeId
     */
    private void sampleAndSwap(int nodeId) {
        Node partner = null;
        Node nodep = entireGraph.get(nodeId);

        // Swap with random neighbors
        if (config.getNodeSelectionPolicy() == NodeSelectionPolicy.HYBRID
                || config.getNodeSelectionPolicy() == NodeSelectionPolicy.LOCAL) {
            Integer neighbours[] = getNeighbors(nodep);
            partner = findPartner(nodeId, neighbours);
        }
        // If local policy fails then randomly sample the entire graph
        if ((config.getNodeSelectionPolicy() == NodeSelectionPolicy.HYBRID && partner == null)
                || config.getNodeSelectionPolicy() == NodeSelectionPolicy.RANDOM) {
            Integer sample[] = getSample(nodeId);
            partner = findPartner(nodeId, sample);
        }

        // Swap the colors
        if (partner != null) {
            int partnerColor = partner.getColor();
            partner.setColor(nodep.getColor());
            nodep.setColor(partnerColor);
            numberOfSwaps++;
        }
    }

    public Node findPartner(int nodeId, Integer[] nodes) {

        Node nodep = entireGraph.get(nodeId);

        Node bestPartner = null;
        double highestBenefit = 0;

        Float a = config.getAlpha();

        for (Integer q : nodes) {
            Node nodeq = entireGraph.get(q);
            int dpp = getDegree(nodep, nodep.getColor());
            int dqq = getDegree(nodeq, nodeq.getColor());
            double oldC = pow(dpp, a) + pow(dqq, a);
            int dpq = getDegree(nodep, nodeq.getColor());
            int dqp = getDegree(nodeq, nodep.getColor());
            double newC = pow(dpq, a) + pow(dqp, a);
            if (acceptSolution(oldC, newC, highestBenefit)) {
                bestPartner = nodeq;
                highestBenefit = newC;
            }
            if (var == Variation.EXPONENTIAL)
                break; // only try one partner
        }

        return bestPartner;
    }

    private boolean acceptSolution(double oldC, double newC, double highestBenefit) {
        switch (var) {
            case LINEAR:
                return newC > highestBenefit && newC * T > oldC;
            case EXPONENTIAL:
                Random r = new Random();
                return r.nextDouble() < exp((1 / oldC - 1 / newC) / T);
            default:
                return false;
        }
    }

    /**
     * The the degreee on the node based on color
     *
     * @param node
     * @param colorId
     * @return how many neighbors of the node have color == colorId
     */
    private int getDegree(Node node, int colorId) {
        int degree = 0;
        for (int neighborId : node.getNeighbours()) {
            Node neighbor = entireGraph.get(neighborId);
            if (neighbor.getColor() == colorId) {
                degree++;
            }
        }
        return degree;
    }
    /**
     * Returns a uniformly random sample of the graph
     *
     * @param currentNodeId
     * @return Returns a uniformly random sample of the graph
     */
    private Integer[] getSample(int currentNodeId) {
        int count = config.getUniformRandomSampleSize();
        int rndId;
        int size = entireGraph.size();
        ArrayList<Integer> rndIds = new ArrayList<Integer>();

        while (true) {
            rndId = nodeIds.get(RandNoGenerator.nextInt(size));
            if (rndId != currentNodeId && !rndIds.contains(rndId)) {
                rndIds.add(rndId);
                count--;
            }

            if (count == 0)
                break;
        }

        Integer[] ids = new Integer[rndIds.size()];
        return rndIds.toArray(ids);
    }

/**
 * Get random neighbors. The number of random neighbors is controlled using
 * -closeByNeighbors command line argument which can be obtained from the config
 * using {@link Config#getRandomNeighborSampleSize()}
 *
 * @param node
 * @return
 */

private Integer[] getNeighbors(Node node) {
    ArrayList<Integer> list = node.getNeighbours();
    int count = config.getRandomNeighborSampleSize();
    int rndId;
    int index;
    int size = list.size();
    ArrayList<Integer> rndIds = new ArrayList<Integer>();

    if (size <= count)
        rndIds.addAll(list);
    else {
        while (true) {
            index = RandNoGenerator.nextInt(size);
            rndId = list.get(index);
            if (!rndIds.contains(rndId)) {
                rndIds.add(rndId);
                count--;
            }

            if (count == 0)
                break;
        }
    }

    Integer[] arr = new Integer[rndIds.size()];
    return rndIds.toArray(arr);
}

/**
 * Generate a report which is stored in a file in the output dir.
 *
 * @throws IOException
 */
private void report() throws IOException {
    int grayLinks = 0;
    int migrations = 0; // number of nodes that have changed the initial color
    int size = entireGraph.size();

    for (int i : entireGraph.keySet()) {
        Node node = entireGraph.get(i);
        int nodeColor = node.getColor();
        ArrayList<Integer> nodeNeighbours = node.getNeighbours();

        if (nodeColor != node.getInitColor()) {
            migrations++;
        }

        if (nodeNeighbours != null) {
            for (int n : nodeNeighbours) {
                Node p = entireGraph.get(n);
                int pColor = p.getColor();

                if (nodeColor != pColor)
                    grayLinks++;
            }
        }
    }

    int edgeCut = grayLinks / 2;

    logger.info("round: " + round +
            ", edge cut:" + edgeCut +
            ", swaps: " + numberOfSwaps +
            ", migrations: " + migrations +
            ", T: " + T);

    saveToFile(edgeCut, migrations);
}
    private void saveToFile(int edgeCuts, int migrations) throws IOException {
        String delimiter = "\t\t";
        String outputFilePath;

        //output file name
        File inputFile = new File(config.getGraphFilePath());
        outputFilePath = config.getOutputDir() +
                File.separator +
                inputFile.getName() + ".txt";

        if (!resultFileCreated) {
            File outputDir = new File(config.getOutputDir());
            if (!outputDir.exists()) {
                if (!outputDir.mkdir()) {
                    throw new IOException("Unable to create the output directory");
                }
            }
            // create folder and result file with header
            String header = "# Migration is number of nodes that have changed color.";
            header += "\n\nRound" + delimiter + "Edge-Cut" + delimiter + "Swaps" + delimiter + "Migrations" + delimiter + "Skipped" + "\n";
            FileIO.write(header, outputFilePath);
            resultFileCreated = true;
        }

        FileIO.append(round + delimiter + (edgeCuts) + delimiter + numberOfSwaps + delimiter + migrations + "\n", outputFilePath);
    }

    private enum Variation {LINEAR, EXPONENTIAL}
}
