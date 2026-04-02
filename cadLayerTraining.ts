/**
 * CAD Layer Training Module
 * Teaches the AI to identify which layers in CAD drawings represent
 * walls, doors, windows, columns, and other building components
 */

import { getDb } from '../db';
import { cadLayerPatterns, InsertCadLayerPattern, cadExtractionHistory, InsertCadExtractionHistory } from '../../drizzle/schema';
import { eq, like, and, desc } from 'drizzle-orm';

// Default layer patterns based on common AutoCAD naming conventions
const DEFAULT_LAYER_PATTERNS: Omit<InsertCadLayerPattern, 'id' | 'createdAt' | 'updatedAt'>[] = [
  // Walls
  { layerPattern: '%WALL%', layerType: 'external_wall', confidence: '70', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%EXT%WALL%', layerType: 'external_wall', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%EXTERNAL%', layerType: 'external_wall', confidence: '75', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%INT%WALL%', layerType: 'internal_wall', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%INTERNAL%', layerType: 'internal_wall', confidence: '70', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%PARTITION%', layerType: 'partition', confidence: '80', source: 'default', confirmationCount: 1, isActive: 'yes' },
  
  // Openings
  { layerPattern: '%DOOR%', layerType: 'door', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%DR%', layerType: 'door', confidence: '60', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%WINDOW%', layerType: 'window', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%WIN%', layerType: 'window', confidence: '70', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%OPENING%', layerType: 'opening', confidence: '80', source: 'default', confirmationCount: 1, isActive: 'yes' },
  
  // Structure
  { layerPattern: '%COLUMN%', layerType: 'column', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%COL%', layerType: 'column', confidence: '70', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%BEAM%', layerType: 'beam', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%SLAB%', layerType: 'slab', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%FOUNDATION%', layerType: 'foundation', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%FOOTING%', layerType: 'foundation', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%STRUCT%', layerType: 'column', confidence: '60', source: 'default', confirmationCount: 1, isActive: 'yes' },
  
  // Annotations
  { layerPattern: '%DIM%', layerType: 'dimension', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%DIMENSION%', layerType: 'dimension', confidence: '95', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%TEXT%', layerType: 'text', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%ANNO%', layerType: 'annotation', confidence: '80', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%NOTE%', layerType: 'annotation', confidence: '75', source: 'default', confirmationCount: 1, isActive: 'yes' },
  
  // Site
  { layerPattern: '%BOUNDARY%', layerType: 'boundary', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%PLOT%', layerType: 'plot', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%SITE%', layerType: 'site', confidence: '80', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%PROPERTY%', layerType: 'boundary', confidence: '75', source: 'default', confirmationCount: 1, isActive: 'yes' },
  
  // MEP
  { layerPattern: '%ELEC%', layerType: 'electrical', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%PLUMB%', layerType: 'plumbing', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%HVAC%', layerType: 'hvac', confidence: '90', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%AC%', layerType: 'hvac', confidence: '60', source: 'default', confirmationCount: 1, isActive: 'yes' },
  
  // Furniture
  { layerPattern: '%FURN%', layerType: 'furniture', confidence: '85', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%FURNITURE%', layerType: 'furniture', confidence: '95', source: 'default', confirmationCount: 1, isActive: 'yes' },
  { layerPattern: '%EQUIP%', layerType: 'equipment', confidence: '80', source: 'default', confirmationCount: 1, isActive: 'yes' },
];

/**
 * Initialize default layer patterns in database
 */
export async function initializeDefaultLayerPatterns(): Promise<void> {
  const db = await getDb();
  if (!db) return;

  try {
    // Check if patterns already exist
    const existing = await db.select().from(cadLayerPatterns).limit(1);
    if (existing.length > 0) {
      console.log('[CAD Training] Layer patterns already initialized');
      return;
    }

    // Insert default patterns
    for (const pattern of DEFAULT_LAYER_PATTERNS) {
      await db.insert(cadLayerPatterns).values(pattern);
    }
    
    console.log(`[CAD Training] Initialized ${DEFAULT_LAYER_PATTERNS.length} default layer patterns`);
  } catch (error) {
    console.error('[CAD Training] Failed to initialize layer patterns:', error);
  }
}

/**
 * Identify layer type based on layer name
 */
export async function identifyLayerType(layerName: string): Promise<{
  layerType: string;
  confidence: number;
  patternMatched: string | null;
}> {
  const db = await getDb();
  if (!db) {
    return { layerType: 'other', confidence: 0, patternMatched: null };
  }

  try {
    // Get all active patterns
    const patterns = await db
      .select()
      .from(cadLayerPatterns)
      .where(eq(cadLayerPatterns.isActive, 'yes'))
      .orderBy(desc(cadLayerPatterns.confidence));

    const upperLayerName = layerName.toUpperCase();

    for (const pattern of patterns) {
      // Convert SQL LIKE pattern to regex
      const regexPattern = pattern.layerPattern
        .replace(/%/g, '.*')
        .replace(/_/g, '.');
      
      const regex = new RegExp(`^${regexPattern}$`, 'i');
      
      if (regex.test(upperLayerName)) {
        return {
          layerType: pattern.layerType,
          confidence: parseFloat(pattern.confidence || '0'),
          patternMatched: pattern.layerPattern,
        };
      }
    }

    return { layerType: 'other', confidence: 0, patternMatched: null };
  } catch (error) {
    console.error('[CAD Training] Error identifying layer type:', error);
    return { layerType: 'other', confidence: 0, patternMatched: null };
  }
}

/**
 * Analyze all layers in a CAD file
 */
export async function analyzeCADLayers(layers: string[]): Promise<{
  layerAnalysis: Array<{
    layerName: string;
    layerType: string;
    confidence: number;
    patternMatched: string | null;
  }>;
  summary: {
    wallLayers: string[];
    doorLayers: string[];
    windowLayers: string[];
    structureLayers: string[];
    dimensionLayers: string[];
    otherLayers: string[];
  };
}> {
  const layerAnalysis = await Promise.all(
    layers.map(async (layerName) => {
      const result = await identifyLayerType(layerName);
      return {
        layerName,
        ...result,
      };
    })
  );

  // Build summary
  const summary = {
    wallLayers: layerAnalysis
      .filter(l => ['external_wall', 'internal_wall', 'partition'].includes(l.layerType))
      .map(l => l.layerName),
    doorLayers: layerAnalysis
      .filter(l => l.layerType === 'door')
      .map(l => l.layerName),
    windowLayers: layerAnalysis
      .filter(l => l.layerType === 'window')
      .map(l => l.layerName),
    structureLayers: layerAnalysis
      .filter(l => ['column', 'beam', 'slab', 'foundation'].includes(l.layerType))
      .map(l => l.layerName),
    dimensionLayers: layerAnalysis
      .filter(l => ['dimension', 'text', 'annotation'].includes(l.layerType))
      .map(l => l.layerName),
    otherLayers: layerAnalysis
      .filter(l => l.layerType === 'other')
      .map(l => l.layerName),
  };

  return { layerAnalysis, summary };
}

/**
 * Learn a new layer pattern from user confirmation
 */
export async function learnLayerPattern(
  layerName: string,
  layerType: InsertCadLayerPattern['layerType'],
  source: 'learned' | 'manual' = 'learned'
): Promise<boolean> {
  const db = await getDb();
  if (!db) return false;

  try {
    // Create a pattern from the layer name
    // Extract common parts and create a wildcard pattern
    const pattern = createPatternFromLayerName(layerName);

    // Check if similar pattern exists
    const existing = await db
      .select()
      .from(cadLayerPatterns)
      .where(
        and(
          eq(cadLayerPatterns.layerPattern, pattern),
          eq(cadLayerPatterns.layerType, layerType)
        )
      )
      .limit(1);

    if (existing.length > 0) {
      // Update confidence and confirmation count
      const current = existing[0];
      const newConfidence = Math.min(99, parseFloat(current.confidence || '50') + 5);
      
      await db
        .update(cadLayerPatterns)
        .set({
          confidence: newConfidence.toString(),
          confirmationCount: (current.confirmationCount || 0) + 1,
        })
        .where(eq(cadLayerPatterns.id, current.id));
    } else {
      // Insert new pattern
      await db.insert(cadLayerPatterns).values({
        layerPattern: pattern,
        layerType,
        confidence: '60',
        confirmationCount: 1,
        source,
        isActive: 'yes',
      });
    }

    return true;
  } catch (error) {
    console.error('[CAD Training] Error learning layer pattern:', error);
    return false;
  }
}

/**
 * Create a pattern from a layer name
 */
function createPatternFromLayerName(layerName: string): string {
  // Remove numbers and create a pattern
  const cleaned = layerName.toUpperCase()
    .replace(/[0-9]+/g, '')
    .replace(/[-_]+/g, '_')
    .trim();

  // If the name contains meaningful words, use them
  const words = cleaned.split('_').filter(w => w.length > 1);
  
  if (words.length > 0) {
    return `%${words.join('%')}%`;
  }
  
  return `%${cleaned}%`;
}

/**
 * Save CAD extraction history for learning and overlay
 */
export async function saveCADExtractionHistory(
  data: Omit<InsertCadExtractionHistory, 'id' | 'createdAt'>
): Promise<number | null> {
  const db = await getDb();
  if (!db) return null;

  try {
    const result = await db.insert(cadExtractionHistory).values(data);
    return result[0].insertId;
  } catch (error) {
    console.error('[CAD Training] Error saving extraction history:', error);
    return null;
  }
}

/**
 * Get CAD extraction history for a project
 */
export async function getCADExtractionHistory(projectId: number): Promise<any[]> {
  const db = await getDb();
  if (!db) return [];

  try {
    const results = await db
      .select()
      .from(cadExtractionHistory)
      .where(eq(cadExtractionHistory.projectId, projectId))
      .orderBy(desc(cadExtractionHistory.createdAt));
    
    return results;
  } catch (error) {
    console.error('[CAD Training] Error getting extraction history:', error);
    return [];
  }
}

/**
 * Get all layer patterns for admin review
 */
export async function getAllLayerPatterns(): Promise<any[]> {
  const db = await getDb();
  if (!db) return [];

  try {
    const patterns = await db
      .select()
      .from(cadLayerPatterns)
      .orderBy(desc(cadLayerPatterns.confidence));
    
    return patterns;
  } catch (error) {
    console.error('[CAD Training] Error getting layer patterns:', error);
    return [];
  }
}

/**
 * Update layer pattern
 */
export async function updateLayerPattern(
  id: number,
  updates: Partial<InsertCadLayerPattern>
): Promise<boolean> {
  const db = await getDb();
  if (!db) return false;

  try {
    await db
      .update(cadLayerPatterns)
      .set(updates)
      .where(eq(cadLayerPatterns.id, id));
    
    return true;
  } catch (error) {
    console.error('[CAD Training] Error updating layer pattern:', error);
    return false;
  }
}

/**
 * Delete layer pattern
 */
export async function deleteLayerPattern(id: number): Promise<boolean> {
  const db = await getDb();
  if (!db) return false;

  try {
    await db
      .delete(cadLayerPatterns)
      .where(eq(cadLayerPatterns.id, id));
    
    return true;
  } catch (error) {
    console.error('[CAD Training] Error deleting layer pattern:', error);
    return false;
  }
}
