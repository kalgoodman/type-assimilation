/*******************************************************************************
 * Copyright (c) 2014 itemis AG and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Matthias Wienand (itemis AG) - initial API & implementation
 *
 *******************************************************************************/
package com.typeassimilation.example;

import org.eclipse.gef4.mvc.fx.parts.FXDefaultFeedbackPartFactory;
import org.eclipse.gef4.mvc.parts.IFeedbackPart;
import org.eclipse.gef4.mvc.parts.IVisualPart;

import javafx.scene.Node;

public class ZestGraphExampleFeedbackPartFactory extends FXDefaultFeedbackPartFactory {

	@Override
	protected IFeedbackPart<Node, ? extends Node> createLinkFeedbackPart(IVisualPart<Node, ? extends Node> anchored,
			IVisualPart<Node, ? extends Node> anchorage, String anchorageRole) {
		return null;
	}

}
